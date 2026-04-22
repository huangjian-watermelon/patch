#include <iostream>
#include <string>
#include <csignal>

#include "../shared/json_config.h"
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

bool LoadConfig(const std::string& path, StreamForwarderConfig& cfg)
{
    JsonConfig json;
    std::string err;
    if (!JsonConfig::LoadFromFile(path, json, err))
    {
        std::cerr << "Load config failed: " << err << "\n";
        return false;
    }

    json.GetString("input_mcast_ip", cfg.input_mcast_ip);
    json.GetUInt16("input_mcast_port", cfg.input_mcast_port);
    json.GetString("output_mcast_ip", cfg.output_mcast_ip);
    json.GetUInt16("output_mcast_port", cfg.output_mcast_port);
    json.GetSize("ring_capacity", cfg.ring_capacity);
    return true;
}

void IgnoreTerminalSignals()
{
    std::signal(SIGINT, SIG_IGN);
    std::signal(SIGHUP, SIG_IGN);
}
}

int main(int argc, char* argv[])
{
    IgnoreTerminalSignals();

    StreamForwarderConfig cfg;
    const std::string config_path = (argc >= 2) ? argv[1] : "forwarder.json";
    if (!LoadConfig(config_path, cfg))
    {
        std::cerr << "Usage: forwarder <config.json>\n";
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
