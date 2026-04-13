#pragma once
#include <cstdint>

static constexpr uint16_t RETRANS_MAGIC = 0x5254;

enum class RetransMsgType : uint16_t
{
    REQUEST  = 1,
    RESPONSE = 2
};

#pragma pack(push, 1)

struct RetransHeader
{
    uint16_t magic;
    uint16_t msg_type;
    uint16_t body_len;
    uint16_t reserved;
};

struct RetransRequestBody
{
    uint64_t start_seq;
    uint16_t count;
};

struct RetransResponseBody
{
    uint64_t seq;
    uint16_t data_len;
};


#pragma pack(pop)
