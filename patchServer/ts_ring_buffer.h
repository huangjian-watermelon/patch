#pragma once

#include <vector>
#include <mutex>
#include <cstdint>
#include <ts_packet.h>

class TsRingBuffer
{
public:
    explicit TsRingBuffer(size_t capacity);

    void Push(const TsPacket& packet);
    bool GetBySeq(uint64_t seq, TsPacket& out_packet);

    uint64_t GetMinSeq() const;
    uint64_t GetMaxSeq() const;
    size_t GetSize() const;
    size_t GetCapacity() const;

private:
    size_t capacity_ = 0;
    std::vector<TsPacket> buffer_;

    size_t writeIndex_ = 0;
    size_t size_ = 0;

    uint64_t minSeq_ = 0;
    uint64_t maxSeq_ = 0;
    bool initialized_ = false;

    mutable std::mutex mutex_;

};
