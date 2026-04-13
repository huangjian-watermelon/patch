#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>

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

    void Init(int req_sock, const sockaddr_in& server_addr) {
        req_sock_ = req_sock;
        server_addr_ = server_addr;
    }

    void OnMissingRange(uint64_t start_seq, uint16_t count) {
        std::lock_guard<std::mutex> lock(mutex_);
        requested_ranges_++;
        requested_packets_ += count;

        const auto now = Clock::now();

        for (uint64_t seq = start_seq; seq < start_seq + count; ++seq) {
            auto it = missing_map_.find(seq);
            if (it != missing_map_.end()) {
                continue;
            }

            MissingPacketInfo info;
            info.seq = seq;
            info.first_request_time = now;
            info.last_request_time = now;
            info.retry_count = 1;
            missing_map_[seq] = info;

            SendRequestUnlocked(seq, 1);
        }
    }

    void OnPacketRecovered(uint64_t seq) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = missing_map_.find(seq);
        if (it != missing_map_.end()) {
            it->second.recovered = true;
            recovered_packets_++;
        }
    }

    void CheckTimeouts(std::vector<uint64_t>& expired_seqs) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now = Clock::now();

        for (auto& kv : missing_map_) {
            auto& info = kv.second;

            if (info.recovered || info.expired) {
                continue;
            }

            auto total_elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - info.first_request_time);

            auto retry_elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - info.last_request_time);

            if (total_elapsed >= total_timeout_) {
                info.expired = true;
                expired_seqs.push_back(info.seq);
                continue;
            }

            if (retry_elapsed >= retry_interval_ && info.retry_count < max_retry_count_) {
                SendRequestUnlocked(info.seq, 1);
                info.last_request_time = now;
                ++info.retry_count;
                retry_requests_++;
            }
        }
    }

    void PrintStats() {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "[RetransRequest][STAT] "
                  << "requested_ranges=" << requested_ranges_
                  << " requested_packets=" << requested_packets_
                  << " retry_requests=" << retry_requests_
                  << " recovered_packets=" << recovered_packets_
                  << " inflight_missing=" << missing_map_.size()
                  << std::endl;
    }

    void Cleanup() {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = missing_map_.begin(); it != missing_map_.end(); ) {
            if (it->second.recovered || it->second.expired) {
                it = missing_map_.erase(it);
            } else {
                ++it;
            }
        }
    }

private:
    void SendRequestUnlocked(uint64_t start_seq, uint16_t count) {
        uint8_t send_buf[sizeof(RetransHeader) + sizeof(RetransRequestBody)] = {0};

        auto* hdr = reinterpret_cast<RetransHeader*>(send_buf);
        hdr->magic = htons(RETRANS_MAGIC);
        hdr->msg_type = htons(static_cast<uint16_t>(RetransMsgType::REQUEST));
        hdr->body_len = htons(sizeof(RetransRequestBody));
        hdr->reserved = 0;

        auto* body = reinterpret_cast<RetransRequestBody*>(send_buf + sizeof(RetransHeader));
        body->start_seq = HostToNet64(start_seq);
        body->count = htons(count);

        ::sendto(req_sock_,
                 send_buf,
                 sizeof(send_buf),
                 0,
                 reinterpret_cast<const sockaddr*>(&server_addr_),
                 sizeof(server_addr_));

        std::cout << "[RetransRequest] request seq=" << start_seq
                  << " count=" << count << std::endl;
    }

private:
    std::mutex mutex_;
    std::unordered_map<uint64_t, MissingPacketInfo> missing_map_;

    int req_sock_ = -1;
    sockaddr_in server_addr_{};

    std::chrono::milliseconds retry_interval_{30};
    std::chrono::milliseconds total_timeout_{60};
    int max_retry_count_ = 2; // 第一次 + 重试一次

    uint64_t requested_ranges_ = 0;
    uint64_t requested_packets_ = 0;
    uint64_t retry_requests_ = 0;
    uint64_t recovered_packets_ = 0;
};
