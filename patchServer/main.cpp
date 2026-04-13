#include <iostream>
#include <thread>
#include <string>
#include <fstream>
#include <regex>
#include "udp_ts_receive.h"
#include "ts_ring_buffer.h"
#include "retrans_server.h"

using namespace std;

namespace  {
struct ServerConfig
{
    std::string input_mcast_ip = "238.1.1.130";
    uint16_t input_mcast_port = 1234;
    std::string output_mcast_ip = "238.1.1.127";
    uint16_t output_mcast_port = 5040;

    std::string req_bind_ip = "0.0.0.0";
    uint16_t req_bind_port = 9000;
    uint16_t retrans_send_port = 9001;
    size_t ring_capacity = 100 * 1024;
};

bool ReadFile(const std::string& path, std::string& content)
{
    std::ifstream ifs(path);
    if (!ifs.is_open())
    {
        return false;
    }
    content.assign((std::istreambuf_iterator<char>(ifs)),
                   std::istreambuf_iterator<char>());
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

bool LoadConfig(const std::string& path, ServerConfig& cfg)
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
    GetString(json, "req_bind_ip", cfg.req_bind_ip);
    GetU16(json, "req_bind_port", cfg.req_bind_port);
    GetU16(json, "retrans_send_port", cfg.retrans_send_port);
    GetSize(json, "ring_capacity", cfg.ring_capacity);
    return true;
}
}

int main(int argc, char* argv[])
{
    ServerConfig cfg;
    const std::string config_path = (argc >= 2) ? argv[1] : "patch_server.json";
    if (!LoadConfig(config_path, cfg))
    {
        std::cerr << "Usage: patchServer <config.json>\n";
        return 1;
    }

    TsRingBuffer ringBuffer(cfg.ring_capacity);

    UdpTsReceiver receiver(ringBuffer);
    RetransServer retrans_server(ringBuffer, cfg.retrans_send_port);

    if (!receiver.Init(cfg.input_mcast_ip, cfg.input_mcast_port))
    {
        std::cerr << "Receiver init failed!\n";
        return 1;
    }

    if (!receiver.InitSend(cfg.output_mcast_ip, cfg.output_mcast_port))
    {
        std::cerr << "Send init failed!\n";
        return 1;
    }

    if (!retrans_server.Init(cfg.req_bind_ip, cfg.req_bind_port))
    {
        std::cerr << "RetransServer init failed!\n";
        return 1;
    }

    std::thread t1([&]() {
        receiver.Run();
    });

    std::thread t2([&]() {
        retrans_server.Run();
    });

    t1.join();
    t2.join();

    return 0;
}
