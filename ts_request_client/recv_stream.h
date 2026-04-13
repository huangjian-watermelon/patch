#ifndef RECV_STREAM_H
#define RECV_STREAM_H

#include <cstdint>

struct StreamPacket
{
    uint64_t seq;
    uint8_t ts_data[188];
};

#endif // RECV_STREAM_H
