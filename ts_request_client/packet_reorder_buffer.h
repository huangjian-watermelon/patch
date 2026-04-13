#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <chrono>
#include <map>
#include <unordered_set>
#include <mutex>
#include <iostream>
#include <cstring>
#include <string>

#include "recv_stream.h"
#include "ts_output_sender.h"

static constexpr uint16_t TS_PACKET_SIZE = 188;
static constexpr size_t MAX_BUFFER_SIZE = 1024;

class PacketReorderBuffer
{
public:
    PacketReorderBuffer() = default;

    ~PacketReorderBuffer()
    {

    }

    bool InitDeliver(const std::string& ip, uint16_t port)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        deliver_sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
        if (deliver_sock_ < 0)
        {
            std::cerr << "[ReorderBuffer] create deliver socket failed!\n";
            return false;
        }

        int ttl = 16;
        ::setsockopt(deliver_sock_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

        in_addr local_if{};
        local_if.s_addr = htonl(INADDR_ANY);
        ::setsockopt(deliver_sock_, IPPROTO_IP, IP_MULTICAST_IF, &local_if, sizeof(local_if));

        std::memset(&deliver_addr_, 0, sizeof(deliver_addr_));
        deliver_addr_.sin_family = AF_INET;
        deliver_addr_.sin_port = port;

        if (::inet_pton(AF_INET, ip.c_str(), &deliver_addr_.sin_addr) != 1)
        {
            std::cerr << "[ReorderBuffer] inet_pton failed, ip = " << ip << '\n';
            return false;
        }

        deliver_enabled_ = true;
        std::cerr << "[ReorderBuffer] deliver enabled -> " << ip << ":" << port << "\n";

        return true;
    }

    void OnPacket(const StreamPacket& pkt)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        ++recv_total_;

        if (!initialized_)
        {
            expected_seq_ = pkt.seq;
            initialized_ = true;
        }

        if (pkt.seq < expected_seq_)
        {
            ++drop_old_;
            return;
        }

        if (buffer_.find(pkt.seq) != buffer_.end())
        {
            ++drop_duplicate_;
            return;
        }

        buffer_.emplace(pkt.seq, pkt);

        if (buffer_.size() > MAX_BUFFER_SIZE)
        {
            std::cout << "[ReorderBuffer] buffer overflow, force skip seq = "
                      << expected_seq_ << std::endl;

            uint64_t skip_seq = expected_seq_;
            expired_seqs_.insert(skip_seq);
        }

        DeliverAvailableLocked();
    }

    void MarkSeqExpired(uint64_t seq)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (seq < expected_seq_)
        {
            return;
        }

        expired_seqs_.insert(seq);
        DeliverAvailableLocked();
    }

    void PrintStats() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        std::cout << "[ReorderBuffer] "
                  << "recv_total=" << recv_total_
                  << " delivered=" << delivered_total_
                  << " drop_old=" << drop_old_
                  << " drop_duplicate=" << drop_duplicate_
                  << " buffer_size=" << buffer_.size()
                  << " expected_seq=" << expected_seq_
                  << '\n';
    }

    void SetSender(TsOutputSender* sender)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        sender_ = sender;
    }

private:
    void DeliverAvailableLocked()
    {
        while (true)
        {
            auto it = buffer_.find(expected_seq_);
            if (it != buffer_.end())
            {
                DeliverPacketLocked(it->second);
                buffer_.erase(it);

                ++expected_seq_;
                ++delivered_total_;
                continue;
            }

            if (expired_seqs_.count(expected_seq_))
            {
                std::cout << "[ReorderBuffer] skip expired seq = " << expected_seq_ << std::endl;

                expired_seqs_.erase(expected_seq_);
                ++expected_seq_;
                continue;
            }
            break;
        }
    }

    void DeliverPacketLocked(const StreamPacket& pkt)
    {
        static uint64_t delivered_count = 0;
        static uint64_t first_seq = 0;
        static bool first = true;

        if (first)
        {
            first_seq = pkt.seq;
            first = false;
        }
        delivered_count++;
        if ((delivered_count % 500) == 0) {
            std::cout << "[DELIVER] count = " << delivered_count
                      << "  first_seq = " << first_seq
                      << "  current_seq = " << pkt.seq
                      << "\n";
        }

        if (sender_)
        {
            sender_->PushTs(pkt.ts_data);
        }
    }

private:
    mutable std::mutex mutex_;
    bool initialized_ = false;
    uint64_t expected_seq_ = 0;
    std::map<uint64_t, StreamPacket> buffer_;

    std::unordered_set<uint64_t> expired_seqs_;

    int deliver_sock_ = -1;
    sockaddr_in deliver_addr_{};
    bool deliver_enabled_ = false;

    uint64_t recv_total_ = 0;
    uint64_t delivered_total_ = 0;
    uint64_t drop_old_ = 0;
    uint64_t drop_duplicate_ = 0;

    TsOutputSender* sender_ = nullptr;
};
