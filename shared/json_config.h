#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>

class JsonConfig
{
public:
    static bool LoadFromFile(const std::string& path, JsonConfig& out, std::string& err);

    bool GetString(const std::string& key, std::string& out) const;
    bool GetUInt16(const std::string& key, uint16_t& out) const;
    bool GetSize(const std::string& key, size_t& out) const;

private:
    std::unordered_map<std::string, std::string> string_values_;
    std::unordered_map<std::string, uint64_t> number_values_;
};
