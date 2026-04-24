#pragma once

#include <arpa/inet.h>
#include <sys/socket.h>

#include <cstdint>
#include <map>
#include <unordered_set>
#include <mutex>
#include <string>
#include <vector>

#include "recv_stream.h"
#include "ts_output_sender.h"

static constexpr uint16_t TS_PACKET_SIZE = 188;
static constexpr size_t MAX_BUFFER_SIZE = 1024;

class PacketReorderBuffer
{
public:
    PacketReorderBuffer() = default;
    ~PacketReorderBuffer();

    bool InitDeliver(const std::string& ip, uint16_t port);
    void OnPacket(const StreamPacket& pkt);
    void MarkSeqExpired(uint64_t seq);
    void ResetForNewSession(uint64_t session_id, uint64_t first_seq);
    void PrintStats() const;
    void SetSender(TsOutputSender* sender);

private:
    struct PendingDelivery
    {
        StreamPacket packet{};
        uint64_t delivery_count = 0;
        uint64_t first_seq = 0;
        bool should_log = false;
    };

    std::vector<PendingDelivery> CollectDeliverablePacketsLocked();
    void DeliverPackets(const std::vector<PendingDelivery>& packets);

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
    uint64_t restart_resync_ = 0;
    uint64_t current_session_id_ = 0;
    uint64_t delivered_count_log_ = 0;
    uint64_t first_seq_log_ = 0;
    bool first_packet_seen_ = false;

    TsOutputSender* sender_ = nullptr;
};
