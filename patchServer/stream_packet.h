#pragma once
#include <cstdint>

struct StreamPacket
{
    uint64_t seq;
    uint8_t ts_data[188];
};
