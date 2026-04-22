#pragma once

#include <unordered_map>
#include <vector>
#include <mutex>
#include <chrono>
#include <cstdint>
#include <atomic>

#include <arpa/inet.h>
#include <sys/socket.h>

#include "retrans_protocol.h"

struct MissingPacketInfo {
    uint64_t seq = 0;
    std::chrono::steady_clock::time_point first_request_time;
    std::chrono::steady_clock::time_point last_request_time;
    int retry_count = 0;
    bool recovered = false;
    bool expired = false;
};

class RetransRequestManager {
public:
    using Clock = std::chrono::steady_clock;

    void Init(int req_sock, const sockaddr_in& server_addr);
    void Configure(std::chrono::milliseconds retry_interval,
                   std::chrono::milliseconds total_timeout,
                   int max_retry_count);
    void OnMissingRange(uint64_t start_seq, uint16_t count);
    void OnPacketRecovered(uint64_t seq);
    void OnSessionChanged(uint64_t session_id);
    void CheckTimeouts(std::vector<uint64_t>& expired_seqs);
    void PrintStats();
    void Cleanup();

private:
    void SendRequestUnlocked(uint64_t start_seq, uint16_t count, uint32_t request_id);
    uint32_t NextRequestId();

private:
    std::mutex mutex_;
    std::unordered_map<uint64_t, MissingPacketInfo> missing_map_;

    int req_sock_ = -1;
    sockaddr_in server_addr_{};

    std::chrono::milliseconds retry_interval_{30};
    std::chrono::milliseconds total_timeout_{60};
    int max_retry_count_ = 2;

    uint64_t requested_ranges_ = 0;
    uint64_t requested_packets_ = 0;
    uint64_t retry_requests_ = 0;
    uint64_t recovered_packets_ = 0;
    std::atomic<uint32_t> next_request_id_{1};
    uint64_t current_session_id_ = 0;
};
