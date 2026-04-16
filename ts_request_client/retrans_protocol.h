#pragma once
#include <cstdint>
#include <arpa/inet.h>

static constexpr uint16_t RETRANS_MAGIC = 0x5254;
static constexpr uint8_t RETRANS_VERSION = 1;

enum class RetransMsgType : uint16_t
{
    REQUEST  = 1,
    RESPONSE = 2
};

#pragma pack(push, 1)

struct RetransHeader
{
    uint16_t magic;
    uint8_t version;
    uint8_t reserved0;
    uint16_t msg_type;
    uint16_t body_len;
    uint32_t request_id;
};

struct RetransRequestBody
{
    uint64_t session_id;
    uint64_t start_seq;
    uint16_t count;
};

struct RetransResponseBody
{
    uint64_t seq;
    uint16_t data_len;
};


#pragma pack(pop)

inline uint64_t HostToNet64(uint64_t v)
{
    const uint32_t hi = htonl(static_cast<uint32_t>(v >> 32));
    const uint32_t lo = htonl(static_cast<uint32_t>(v & 0xFFFFFFFFULL));
    return (static_cast<uint64_t>(lo) << 32) | hi;
}

inline uint64_t NetToHost64(uint64_t v)
{
    const uint32_t hi = ntohl(static_cast<uint32_t>(v >> 32));
    const uint32_t lo = ntohl(static_cast<uint32_t>(v & 0xFFFFFFFFULL));
    return (static_cast<uint64_t>(lo) << 32) | hi;
}
