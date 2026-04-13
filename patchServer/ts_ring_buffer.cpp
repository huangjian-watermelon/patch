#include "ts_ring_buffer.h"
#include <iostream>

TsRingBuffer::TsRingBuffer(size_t capacity)
    : capacity_(capacity)
{
    buffer_.reserve(capacity);
}

void TsRingBuffer::Push(const TsPacket &packet)
{
    std::lock_guard<std::mutex> lock(mutex_);

    buffer_[writeIndex_] = packet;

    writeIndex_ = (writeIndex_ + 1) % capacity_;

    if (size_ < capacity_) {
        ++size_;
    }

    if (!initialized_) {
        minSeq_ = packet.seq;
        maxSeq_ = packet.seq;
        initialized_ = true;
    } else {
        maxSeq_ = packet.seq;
        if (size_ == capacity_) {
            minSeq_ = maxSeq_ - capacity_ + 1;
        }
    }
}

bool TsRingBuffer::GetBySeq(uint64_t seq, TsPacket &out_packet)
{
    std::lock_guard<std::mutex> lock(mutex_);

    if (initialized_ == false)
        return false;

    if (seq < minSeq_ || seq > maxSeq_)
        return false;

    size_t index = seq % capacity_;
    if (buffer_[index].seq != seq)
        return false;

    out_packet = buffer_[index];
    return true;
}

uint64_t TsRingBuffer::GetMinSeq() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_ ? minSeq_ : 0;
}

uint64_t TsRingBuffer::GetMaxSeq() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return initialized_ ? maxSeq_ : 0;
}

size_t TsRingBuffer::GetSize() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return size_;
}

size_t TsRingBuffer::GetCapacity() const
{
    return capacity_;
}




