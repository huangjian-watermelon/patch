#pragma once
#include <stdint.h>

struct TsParsedHeader
{
    uint8_t sync_byte = 0;  // 8 bits
    uint8_t transport_error_indicator = 0;  // 1 bit
    uint8_t payload_unit_start_indicator = 0;   // 1 bit
    uint8_t transport_priority = 0; // 1 bit
    uint16_t pid = 0;   // 13 bits
    uint8_t transport_scrambling_control = 0;   // 2 bits
    uint8_t adaptation_field_control = 0;   // 2 bits
    uint8_t continuity_counter = 0; // 4 bits
};

inline bool ParseTsHeader(const uint8_t* data, TsParsedHeader& h)
{
    if (data == nullptr)
        return false;

    if (data[0] != 0x47)
        return false;

    h.sync_byte = data[0];
    h.transport_error_indicator = (data[1] >> 7) & 0x01;
    h.payload_unit_start_indicator = (data[1] >> 6) & 0x01;
    h.transport_priority = (data[1] >> 5) & 0x01;
    h.pid = static_cast<uint16_t>(((data[1] & 0x1F) << 8) | data[2]);
    h.transport_scrambling_control = (data[3] >> 6) & 0x03;
    h.adaptation_field_control = (data[3] >> 4) & 0x03;
    h.continuity_counter = data[3] & 0x0F;

    return true;
}
