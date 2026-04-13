#pragma once

#include <cstdint>

static constexpr int TS_PACKET_SIZE = 188;

struct TsPacket
{
    uint64_t seq = 0;
    uint64_t recv_time_us = 0;
    uint16_t data_len = TS_PACKET_SIZE;
    uint8_t data[TS_PACKET_SIZE] = {0};
};

