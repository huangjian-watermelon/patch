#pragma once

#include <cstdint>
#include <unordered_map>
#include <iostream>
#include "ts_parsed_header.h"

struct PidStats
{
    uint64_t packet_count = 0;   // 该 PID 收到的包数
    uint64_t loss_count = 0;     // 该 PID 推断出的丢包数
    uint8_t  last_cc = 0;        // 上一次收到的 cc
    bool     has_last_cc = false;
};

class TsLossDetector
{
public:
    void OnPacket(uint64_t seq, const TsParsedHeader& header);
    void PrintStats() const;
    void Reset();

private:
    bool HasPayload(const TsParsedHeader& header) const;
    uint8_t CalcExpectedCc(uint8_t last_cc) const;

private:
    std::unordered_map<uint16_t, PidStats> pid_stats_;
    uint64_t total_packets_ = 0;
    uint64_t total_loss_ = 0;
};
