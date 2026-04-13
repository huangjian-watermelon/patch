#include "ts_loss_detector.h"

bool TsLossDetector::HasPayload(const TsParsedHeader& header) const
{
    return (header.adaptation_field_control == 1 ||
            header.adaptation_field_control == 3);
}

uint8_t TsLossDetector::CalcExpectedCc(uint8_t last_cc) const
{
    return static_cast<uint8_t>((last_cc + 1) & 0x0F);
}

void TsLossDetector::OnPacket(uint64_t seq, const TsParsedHeader& header)
{
    ++total_packets_;

    // TEI=1 说明该 TS 包有传输错误，通常可以单独记日志
    if (header.transport_error_indicator)
    {
        std::cout << "[WARN] seq=" << seq
                  << " pid=" << header.pid
                  << " transport_error_indicator=1"
                  << std::endl;
    }

    PidStats& stats = pid_stats_[header.pid];
    ++stats.packet_count;

    // 只对有 payload 的包做 continuity 检测
    if (!HasPayload(header))
    {
        return;
    }

    if (!stats.has_last_cc)
    {
        stats.last_cc = header.continuity_counter;
        stats.has_last_cc = true;
        return;
    }

    uint8_t expected_cc = CalcExpectedCc(stats.last_cc);
    uint8_t actual_cc = header.continuity_counter;

    if (actual_cc != expected_cc)
    {
        // 计算中间跳过了几个 cc，范围 0~15 循环
        uint8_t diff = static_cast<uint8_t>((actual_cc - expected_cc) & 0x0F);

        // diff == 0 理论上表示重复包或异常情况
        // 这里只做简单处理：至少记 1 次异常，但不把 0 当成丢 16 个
        uint64_t inferred_loss = (diff == 0) ? 1 : diff;

        stats.loss_count += inferred_loss;
        total_loss_ += inferred_loss;

        std::cout << "[LOSS] pid=" << header.pid
                  << " seq=" << seq
                  << " expected_cc=" << static_cast<int>(expected_cc)
                  << " actual_cc=" << static_cast<int>(actual_cc)
                  << " inferred_loss=" << inferred_loss
                  << std::endl;
    }

    stats.last_cc = actual_cc;
}

void TsLossDetector::PrintStats() const
{
    std::cout << "========== TS LOSS STATS ==========" << std::endl;
    std::cout << "total_packets=" << total_packets_
              << " total_loss=" << total_loss_
              << std::endl;

    for (const auto& kv : pid_stats_)
    {
        uint16_t pid = kv.first;
        const PidStats& s = kv.second;

        std::cout << "pid=" << pid
                  << " packets=" << s.packet_count
                  << " loss=" << s.loss_count;

        if (s.has_last_cc)
        {
            std::cout << " last_cc=" << static_cast<int>(s.last_cc);
        }

        std::cout << std::endl;
    }
}

void TsLossDetector::Reset()
{
    pid_stats_.clear();
    total_packets_ = 0;
    total_loss_ = 0;
}
