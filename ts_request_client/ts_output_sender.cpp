#include "ts_output_sender.h"

#include <arpa/inet.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>

TsOutputSender::TsOutputSender()
{
    fp_ = fopen("out.ts", "wb");
    if (!fp_)
    {
        perror("fopen out.ts");
    }
}

TsOutputSender::~TsOutputSender()
{
    Stop();
}

bool TsOutputSender::Init(const std::string& multicast_ip,
                          uint16_t port,
                          const std::string& local_if_ip)
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

void TsOutputSender::Start()
{
    running_ = true;
    worker_ = std::thread(&TsOutputSender::SendLoop, this);
}

void TsOutputSender::Stop()
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

void TsOutputSender::PushTs(const uint8_t* ts188)
{
    if (!ts188)
    {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);

    std::array<uint8_t, kTsPacketSize> pkt{};
    std::memcpy(pkt.data(), ts188, kTsPacketSize);
    queue_.push_back(std::move(pkt));

    ++recv_ts_count_;
    cv_.notify_one();
}

void TsOutputSender::ResetForNewSession()
{
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.clear();
    started_ = false;
    std::cout << "[TsOutputSender] reset for new session" << std::endl;
}

void TsOutputSender::PrintStats() const
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

void TsOutputSender::SendLoop()
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

        if (!running_)
        {
            break;
        }

        if (!started_)
        {
            started_ = true;
            std::cout << "[TsOutputSender] startup buffer ready, queue_ts="
                      << queue_.size() << '\n';
        }

        if (queue_.size() < kBatchTsCount)
        {
            continue;
        }

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
    }
}
