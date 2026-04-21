#include "retrans_request_manager.h"

#include <cstring>
#include <iostream>
#include <algorithm>

void RetransRequestManager::Init(int req_sock, const sockaddr_in& server_addr)
{
    req_sock_ = req_sock;
    server_addr_ = server_addr;
}

void RetransRequestManager::OnMissingRange(uint64_t start_seq, uint16_t count)
{
    std::lock_guard<std::mutex> lock(mutex_);
    requested_ranges_++;
    requested_packets_ += count;

    const auto now = Clock::now();

    bool has_new_missing = false;
    for (uint64_t seq = start_seq; seq < start_seq + count; ++seq)
    {
        auto it = missing_map_.find(seq);
        if (it != missing_map_.end())
        {
            continue;
        }

        MissingPacketInfo info;
        info.seq = seq;
        info.first_request_time = now;
        info.last_request_time = now;
        info.retry_count = 1;
        missing_map_[seq] = info;
        has_new_missing = true;
    }

    if (has_new_missing)
    {
        SendRequestUnlocked(start_seq, count, NextRequestId());
    }
}

void RetransRequestManager::OnPacketRecovered(uint64_t seq)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = missing_map_.find(seq);
    if (it != missing_map_.end())
    {
        it->second.recovered = true;
        recovered_packets_++;
    }
}

void RetransRequestManager::OnSessionChanged(uint64_t session_id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (current_session_id_ == session_id)
    {
        return;
    }

    current_session_id_ = session_id;
    missing_map_.clear();
    std::cout << "[RetransRequest] switch session_id=" << current_session_id_
              << ", clear missing state" << std::endl;
}

void RetransRequestManager::CheckTimeouts(std::vector<uint64_t>& expired_seqs)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto now = Clock::now();
    std::vector<uint64_t> retry_seqs;

    for (auto& kv : missing_map_)
    {
        auto& info = kv.second;

        if (info.recovered || info.expired)
        {
            continue;
        }

        auto total_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - info.first_request_time);

        auto retry_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - info.last_request_time);

        if (total_elapsed >= total_timeout_)
        {
            info.expired = true;
            expired_seqs.push_back(info.seq);
            continue;
        }

        if (retry_elapsed >= retry_interval_ && info.retry_count < max_retry_count_)
        {
            info.last_request_time = now;
            ++info.retry_count;
            retry_requests_++;
            retry_seqs.push_back(info.seq);
        }
    }

    if (retry_seqs.empty())
    {
        return;
    }

    std::sort(retry_seqs.begin(), retry_seqs.end());
    retry_seqs.erase(std::unique(retry_seqs.begin(), retry_seqs.end()), retry_seqs.end());

    uint64_t start = retry_seqs[0];
    uint64_t prev = retry_seqs[0];
    for (size_t i = 1; i <= retry_seqs.size(); ++i)
    {
        const bool end = (i == retry_seqs.size());
        const uint64_t cur = end ? 0 : retry_seqs[i];

        if (!end && cur == prev + 1)
        {
            prev = cur;
            continue;
        }

        const uint64_t range_len = prev - start + 1;
        uint64_t sent = 0;
        while (sent < range_len)
        {
            const uint16_t batch = static_cast<uint16_t>(std::min<uint64_t>(range_len - sent, 1024));
            SendRequestUnlocked(start + sent, batch, NextRequestId());
            sent += batch;
        }

        if (!end)
        {
            start = cur;
            prev = cur;
        }
    }
}

void RetransRequestManager::PrintStats()
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::cout << "[RetransRequest][STAT] "
              << "requested_ranges=" << requested_ranges_
              << " requested_packets=" << requested_packets_
              << " retry_requests=" << retry_requests_
              << " recovered_packets=" << recovered_packets_
              << " inflight_missing=" << missing_map_.size()
              << std::endl;
}

void RetransRequestManager::Cleanup()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto it = missing_map_.begin(); it != missing_map_.end();)
    {
        if (it->second.recovered || it->second.expired)
        {
            it = missing_map_.erase(it);
        }
        else
        {
            ++it;
        }
    }
}

void RetransRequestManager::SendRequestUnlocked(uint64_t start_seq, uint16_t count, uint32_t request_id)
{
    uint8_t send_buf[sizeof(RetransHeader) + sizeof(RetransRequestBody)] = {0};

    auto* hdr = reinterpret_cast<RetransHeader*>(send_buf);
    hdr->magic = htons(RETRANS_MAGIC);
    hdr->version = RETRANS_VERSION;
    hdr->reserved0 = 0;
    hdr->msg_type = htons(static_cast<uint16_t>(RetransMsgType::REQUEST));
    hdr->body_len = htons(sizeof(RetransRequestBody));
    hdr->request_id = htonl(request_id);

    auto* body = reinterpret_cast<RetransRequestBody*>(send_buf + sizeof(RetransHeader));
    body->session_id = HostToNet64(current_session_id_);
    body->start_seq = HostToNet64(start_seq);
    body->count = htons(count);

    ::sendto(req_sock_,
             send_buf,
             sizeof(send_buf),
             0,
             reinterpret_cast<const sockaddr*>(&server_addr_),
             sizeof(server_addr_));

    std::cout << "[RetransRequest] request_id=" << request_id
              << " seq=" << start_seq
              << " count=" << count << std::endl;
}

uint32_t RetransRequestManager::NextRequestId()
{
    return next_request_id_.fetch_add(1);
}
