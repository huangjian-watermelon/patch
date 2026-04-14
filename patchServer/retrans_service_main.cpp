#include <iostream>
#include <string>
#include <thread>

#include "../shared/json_config.h"
#include "retrans_server.h"
#include "ts_ring_buffer.h"
#include "udp_ts_receive.h"

namespace {
struct RetransServiceConfig
{
    std::string input_mcast_ip = "238.1.1.130";
    uint16_t input_mcast_port = 1234;

    std::string req_bind_ip = "0.0.0.0";
    uint16_t req_bind_port = 9000;
    uint16_t retrans_send_port = 9001;

    size_t ring_capacity = 100 * 1024;
};

bool LoadConfig(const std::string& path, RetransServiceConfig& cfg)
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
    json.GetString("req_bind_ip", cfg.req_bind_ip);
    json.GetUInt16("req_bind_port", cfg.req_bind_port);
    json.GetUInt16("retrans_send_port", cfg.retrans_send_port);
    json.GetSize("ring_capacity", cfg.ring_capacity);
    return true;
}
}

int main(int argc, char* argv[])
{
    RetransServiceConfig cfg;
    const std::string config_path = (argc >= 2) ? argv[1] : "retrans_service.json";
    if (!LoadConfig(config_path, cfg))
    {
        std::cerr << "Usage: patchRetransServer <config.json>\n";
        return 1;
    }

    TsRingBuffer ring_buffer(cfg.ring_capacity);
    UdpTsReceiver receiver(ring_buffer);
    RetransServer retrans_server(ring_buffer, cfg.retrans_send_port);

    if (!receiver.Init(cfg.input_mcast_ip, cfg.input_mcast_port))
    {
        std::cerr << "Receiver init failed!\n";
        return 1;
    }

    if (!retrans_server.Init(cfg.req_bind_ip, cfg.req_bind_port))
    {
        std::cerr << "RetransServer init failed!\n";
        return 1;
    }

    std::thread t1([&]() { receiver.Run(); });
    std::thread t2([&]() { retrans_server.Run(); });

    t1.join();
    t2.join();
    return 0;
}
