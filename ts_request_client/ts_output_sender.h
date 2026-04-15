#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <array>
#include <cstdio>

class TsOutputSender
{
public:
    static constexpr size_t kTsPacketSize = 188;
    static constexpr size_t kBatchTsCount = 7;
    static constexpr size_t kUdpPayloadSize = kTsPacketSize * kBatchTsCount;
    static constexpr size_t kStartupBufferTsCount = 500;

    TsOutputSender();
    ~TsOutputSender();

    bool Init(const std::string& multicast_ip,
              uint16_t port,
              const std::string& local_if_ip = "");

    void Start();
    void Stop();
    void PushTs(const uint8_t* ts188);
    void ResetForNewSession();
    void PrintStats() const;

private:
    void SendLoop();

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
