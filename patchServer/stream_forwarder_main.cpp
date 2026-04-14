#include <fstream>
#include <iostream>
#include <regex>
#include <string>

#include "ts_ring_buffer.h"
#include "udp_ts_receive.h"

namespace {
struct StreamForwarderConfig
{
    std::string input_mcast_ip = "238.1.1.130";
    uint16_t input_mcast_port = 1234;
    std::string output_mcast_ip = "238.1.1.127";
    uint16_t output_mcast_port = 5040;
    size_t ring_capacity = 100 * 1024;
};

bool ReadFile(const std::string& path, std::string& content)
{
    std::ifstream ifs(path);
    if (!ifs.is_open())
    {
        return false;
    }
    content.assign((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
    return true;
}

bool GetString(const std::string& json, const std::string& key, std::string& out)
{
    const std::regex re("\"" + key + "\"\\s*:\\s*\"([^\"]*)\"");
    std::smatch m;
    if (!std::regex_search(json, m, re) || m.size() < 2)
    {
        return false;
    }
    out = m[1].str();
    return true;
}

bool GetU16(const std::string& json, const std::string& key, uint16_t& out)
{
    const std::regex re("\"" + key + "\"\\s*:\\s*(\\d+)");
    std::smatch m;
    if (!std::regex_search(json, m, re) || m.size() < 2)
    {
        return false;
    }
    out = static_cast<uint16_t>(std::stoi(m[1].str()));
    return true;
}

bool GetSize(const std::string& json, const std::string& key, size_t& out)
{
    const std::regex re("\"" + key + "\"\\s*:\\s*(\\d+)");
    std::smatch m;
    if (!std::regex_search(json, m, re) || m.size() < 2)
    {
        return false;
    }
    out = static_cast<size_t>(std::stoull(m[1].str()));
    return true;
}

bool LoadConfig(const std::string& path, StreamForwarderConfig& cfg)
{
    std::string json;
    if (!ReadFile(path, json))
    {
        std::cerr << "Failed to open config file: " << path << "\n";
        return false;
    }

    GetString(json, "input_mcast_ip", cfg.input_mcast_ip);
    GetU16(json, "input_mcast_port", cfg.input_mcast_port);
    GetString(json, "output_mcast_ip", cfg.output_mcast_ip);
    GetU16(json, "output_mcast_port", cfg.output_mcast_port);
    GetSize(json, "ring_capacity", cfg.ring_capacity);
    return true;
}
}

int main(int argc, char* argv[])
{
    StreamForwarderConfig cfg;
    const std::string config_path = (argc >= 2) ? argv[1] : "stream_forwarder.json";
    if (!LoadConfig(config_path, cfg))
    {
        std::cerr << "Usage: patchStreamForwarder <config.json>\n";
        return 1;
    }

    TsRingBuffer ring_buffer(cfg.ring_capacity);
    UdpTsReceiver receiver(ring_buffer);

    if (!receiver.Init(cfg.input_mcast_ip, cfg.input_mcast_port))
    {
        std::cerr << "Receiver init failed!\n";
        return 1;
    }

    if (!receiver.InitSend(cfg.output_mcast_ip, cfg.output_mcast_port))
    {
        std::cerr << "Forward init failed!\n";
        return 1;
    }

    receiver.Run();
    return 0;
}
