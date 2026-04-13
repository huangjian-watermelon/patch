#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <array>

namespace  {
    constexpr size_t kTsPacketSize = 188;
    constexpr size_t kBatchTsCount = 7;
    constexpr size_t kUdpPayloadSize = kTsPacketSize * kBatchTsCount; // 1316

// 起播前先缓存多少个 TS 包。300 个包大约 56KB，够朴素也够稳。
    constexpr size_t kStartupBufferTsCount = 500;

// 每发一个 1316 UDP 包后的间隔。先给个保守值，后面再按码率调。
    constexpr int kSendIntervalMs = 2;
}

class TsOutputSender
{
public:
    TsOutputSender()
    {
        fp_ = fopen("out.ts", "wb");
        if (!fp_)
        {
            perror("fopen out.ts");
        }
    }

    ~TsOutputSender()
    {
        Stop();
    }

    bool Init(const std::string& multicast_ip,
              uint16_t port,
              const std::string& local_if_ip = "")
    {
        sockfd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (sockfd_ < 0)
        {
            perror("socket");
            return false;
        }

        int ttl = 16;
        if (::setsockopt(sockfd_, IPPROTO_IP, IP_MULTICAST_TTL,
                         &ttl, sizeof(ttl)) < 0)
        {
            perror("setsockopt IP_MULTICAST_TTL");
            return false;
        }

        int loop = 1;
        if (::setsockopt(sockfd_, IPPROTO_IP, IP_MULTICAST_LOOP,
                         &loop, sizeof(loop)) < 0)
        {
            perror("setsockopt IP_MULTICAST_LOOP");
            return false;
        }

        in_addr local_if{};
        if (!local_if_ip.empty())
        {
            if (::inet_pton(AF_INET, local_if_ip.c_str(), &local_if) != 1)
            {
                std::cerr << "[TsOutputSender] invalid local_if_ip=" << local_if_ip << '\n';
                return false;
            }
        }
        else
        {
            local_if.s_addr = htonl(INADDR_ANY);
        }

        if (::setsockopt(sockfd_, IPPROTO_IP, IP_MULTICAST_IF,
                         &local_if, sizeof(local_if)) < 0)
        {
            perror("setsockopt IP_MULTICAST_IF");
            return false;
        }

        std::memset(&dest_addr_, 0, sizeof(dest_addr_));
        dest_addr_.sin_family = AF_INET;
        dest_addr_.sin_port = htons(port);
        if (::inet_pton(AF_INET, multicast_ip.c_str(), &dest_addr_.sin_addr) != 1)
        {
            std::cerr << "[TsOutputSender] invalid multicast ip=" << multicast_ip << '\n';
            return false;
        }

        std::cout << "[TsOutputSender] output -> "
                  << multicast_ip << ":" << port << '\n';
        return true;
    }

    void Start()
    {
        running_ = true;
        worker_ = std::thread(&TsOutputSender::SendLoop, this);
    }

    void Stop()
    {
        running_ = false;
        cv_.notify_all();

        if (worker_.joinable())
        {
            worker_.join();
        }

        if (sockfd_ >= 0)
        {
            ::close(sockfd_);
            sockfd_ = -1;
        }

        if (fp_)
        {
            fclose(fp_);
            fp_ = nullptr;
        }
    }

    // 外部每来一个已经“可交付”的 TS，就调用一次
    void PushTs(const uint8_t* ts188)
    {
        if (!ts188)
        {
            return;
        }

//        if (fp_)
//        {
//            fwrite(ts188, 1, 188, fp_);
//        }

        std::lock_guard<std::mutex> lock(mutex_);

        std::array<uint8_t, kTsPacketSize> pkt{};
        std::memcpy(pkt.data(), ts188, kTsPacketSize);
        queue_.push_back(std::move(pkt));

        ++recv_ts_count_;
        cv_.notify_one();
    }

    void PrintStats() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "[TsOutputSender] "
                  << "recv_ts=" << recv_ts_count_
                  << " sent_udp=" << sent_udp_count_
                  << " sent_ts=" << sent_ts_count_
                  << " queue_ts=" << queue_.size()
                  << " started=" << started_
                  << '\n';
    }

private:
    void SendLoop()
    {
        std::array<uint8_t, kUdpPayloadSize> udp_buf{};

        while (running_)
        {
            std::unique_lock<std::mutex> lock(mutex_);

            cv_.wait(lock, [&]() {
                if (!running_) return true;
                if (!started_) return queue_.size() >= kStartupBufferTsCount;
                return queue_.size() >= kBatchTsCount;
            });


            // 起播缓冲：先攒够一定量再开始发
            if (!started_)
            {
                started_ = true;
                std::cout << "[TsOutputSender] startup buffer ready, queue_ts="
                          << queue_.size() << '\n';
            }

            // 如果已经开始发送，但队列太少，可以稍微等一下，避免忽快忽慢
//            if (queue_.size() < kBatchTsCount)
//            {
//                lock.unlock();
//                std::this_thread::sleep_for(std::chrono::milliseconds(2));
//                continue;
//            }

            for (size_t i = 0; i < kBatchTsCount; ++i)
            {
                const auto& one = queue_.front();
                std::memcpy(udp_buf.data() + i * kTsPacketSize,
                            one.data(),
                            kTsPacketSize);
                queue_.pop_front();
            }

            lock.unlock();

            ssize_t ret = ::sendto(sockfd_,
                                   udp_buf.data(),
                                   udp_buf.size(),
                                   0,
                                   reinterpret_cast<sockaddr*>(&dest_addr_),
                                   sizeof(dest_addr_));

            if (ret < 0)
            {
                perror("sendto failed!!!");
            }
            else
            {
                std::lock_guard<std::mutex> stat_lock(mutex_);
                ++sent_udp_count_;
                sent_ts_count_ += kBatchTsCount;
            }

            // 节奏控制：先粗暴稳定一点
//            std::this_thread::sleep_for(std::chrono::milliseconds(kSendIntervalMs));
        }
    }

private:
    int sockfd_ = -1;
    sockaddr_in dest_addr_{};

    std::atomic<bool> running_{false};
    std::thread worker_;

    mutable std::mutex mutex_;
    std::condition_variable cv_;

    std::deque<std::array<uint8_t, kTsPacketSize>> queue_;

    bool started_ = false;

    FILE* fp_ = nullptr;

    uint64_t recv_ts_count_ = 0;
    uint64_t sent_udp_count_ = 0;
    uint64_t sent_ts_count_ = 0;
};
