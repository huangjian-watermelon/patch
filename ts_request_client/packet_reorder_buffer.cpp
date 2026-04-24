#include "packet_reorder_buffer.h"

#include <cstring>
#include <iostream>
#include <unistd.h>

namespace {
constexpr uint64_t kSeqResetThreshold = MAX_BUFFER_SIZE * 8;
}

PacketReorderBuffer::~PacketReorderBuffer()
{
    if (deliver_sock_ >= 0)
    {
        ::close(deliver_sock_);
        deliver_sock_ = -1;
    }
}

bool PacketReorderBuffer::InitDeliver(const std::string& ip, uint16_t port)
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
    deliver_addr_.sin_port = htons(port);

    if (::inet_pton(AF_INET, ip.c_str(), &deliver_addr_.sin_addr) != 1)
    {
        std::cerr << "[ReorderBuffer] inet_pton failed, ip = " << ip << '\n';
        return false;
    }

    deliver_enabled_ = true;
    std::cerr << "[ReorderBuffer] deliver enabled -> " << ip << ":" << port << "\n";

    return true;
}

void PacketReorderBuffer::OnPacket(const StreamPacket& pkt)
{
    std::vector<PendingDelivery> ready_packets;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        ++recv_total_;

        if (initialized_ && expected_seq_ > pkt.seq &&
            (expected_seq_ - pkt.seq) > kSeqResetThreshold)
        {
            std::cout << "[ReorderBuffer] detect seq reset/restart, expected_seq="
                      << expected_seq_ << " new_seq=" << pkt.seq << std::endl;

            buffer_.clear();
            expired_seqs_.clear();
            expected_seq_ = pkt.seq;
            ++restart_resync_;
        }

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

        ready_packets = CollectDeliverablePacketsLocked();
    }

    DeliverPackets(ready_packets);
}

void PacketReorderBuffer::MarkSeqExpired(uint64_t seq)
{
    std::vector<PendingDelivery> ready_packets;

    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (seq < expected_seq_)
        {
            return;
        }

        expired_seqs_.insert(seq);
        ready_packets = CollectDeliverablePacketsLocked();
    }

    DeliverPackets(ready_packets);
}

void PacketReorderBuffer::ResetForNewSession(uint64_t session_id, uint64_t first_seq)
{
    TsOutputSender* sender = nullptr;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        current_session_id_ = session_id;
        buffer_.clear();
        expired_seqs_.clear();
        initialized_ = true;
        expected_seq_ = first_seq;
        ++restart_resync_;
        delivered_count_log_ = 0;
        first_seq_log_ = 0;
        first_packet_seen_ = false;
        sender = sender_;
    }

    if (sender)
    {
        sender->ResetForNewSession();
    }

    std::cout << "[ReorderBuffer] switch session_id=" << session_id
              << " reset expected_seq=" << first_seq << std::endl;
}

void PacketReorderBuffer::PrintStats() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::cout << "[ReorderBuffer] "
              << "recv_total=" << recv_total_
              << " delivered=" << delivered_total_
              << " drop_old=" << drop_old_
              << " drop_duplicate=" << drop_duplicate_
              << " restart_resync=" << restart_resync_
              << " session_id=" << current_session_id_
              << " buffer_size=" << buffer_.size()
              << " expected_seq=" << expected_seq_
              << '\n';
}

void PacketReorderBuffer::SetSender(TsOutputSender* sender)
{
    std::lock_guard<std::mutex> lock(mutex_);
    sender_ = sender;
}

std::vector<PacketReorderBuffer::PendingDelivery> PacketReorderBuffer::CollectDeliverablePacketsLocked()
{
    std::vector<PendingDelivery> ready_packets;

    while (true)
    {
        auto it = buffer_.find(expected_seq_);
        if (it != buffer_.end())
        {
            if (!first_packet_seen_)
            {
                first_seq_log_ = it->second.seq;
                first_packet_seen_ = true;
            }

            ++delivered_count_log_;
            ++delivered_total_;

            PendingDelivery delivery;
            delivery.packet = it->second;
            delivery.delivery_count = delivered_count_log_;
            delivery.first_seq = first_seq_log_;
            delivery.should_log = (delivered_count_log_ % 500) == 0;
            ready_packets.push_back(delivery);

            buffer_.erase(it);
            ++expected_seq_;
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

    return ready_packets;
}

void PacketReorderBuffer::DeliverPackets(const std::vector<PendingDelivery>& packets)
{
    TsOutputSender* sender = nullptr;

    {
        std::lock_guard<std::mutex> lock(mutex_);
        sender = sender_;
    }

    for (const auto& item : packets)
    {
        if (item.should_log)
        {
            std::cout << "[DELIVER] count = " << item.delivery_count
                      << "  first_seq = " << item.first_seq
                      << "  current_seq = " << item.packet.seq
                      << "\n";
        }

        if (sender)
        {
            sender->PushTs(item.packet.ts_data);
        }
    }
}
