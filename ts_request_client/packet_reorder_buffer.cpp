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

    DeliverAvailableLocked();
}

void PacketReorderBuffer::MarkSeqExpired(uint64_t seq)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (seq < expected_seq_)
    {
        return;
    }

    expired_seqs_.insert(seq);
    DeliverAvailableLocked();
}

void PacketReorderBuffer::ResetForNewSession(uint64_t session_id, uint64_t first_seq)
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

    if (sender_)
    {
        sender_->ResetForNewSession();
    }

    std::cout << "[ReorderBuffer] switch session_id=" << current_session_id_
              << " reset expected_seq=" << expected_seq_ << std::endl;
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

void PacketReorderBuffer::DeliverAvailableLocked()
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

void PacketReorderBuffer::DeliverPacketLocked(const StreamPacket& pkt)
{
    if (!first_packet_seen_)
    {
        first_seq_log_ = pkt.seq;
        first_packet_seen_ = true;
    }
    ++delivered_count_log_;
    if ((delivered_count_log_ % 500) == 0)
    {
        std::cout << "[DELIVER] count = " << delivered_count_log_
                  << "  first_seq = " << first_seq_log_
                  << "  current_seq = " << pkt.seq
                  << "\n";
    }

    if (sender_)
    {
        sender_->PushTs(pkt.ts_data);
    }
}
