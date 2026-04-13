#include <iostream>
#include <thread>
#include <string>
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

uint16_t ToU16(const std::string& s)
{
    return static_cast<uint16_t>(std::stoi(s));
}

size_t ToSize(const std::string& s)
{
    return static_cast<size_t>(std::stoull(s));
}

bool ParseArgs(int argc, char* argv[], ServerConfig& cfg)
{
    for (int i = 1; i < argc; ++i)
    {
        const std::string arg = argv[i];
        if (arg == "--input-mcast-ip" && i + 1 < argc) cfg.input_mcast_ip = argv[++i];
        else if (arg == "--input-mcast-port" && i + 1 < argc) cfg.input_mcast_port = ToU16(argv[++i]);
        else if (arg == "--output-mcast-ip" && i + 1 < argc) cfg.output_mcast_ip = argv[++i];
        else if (arg == "--output-mcast-port" && i + 1 < argc) cfg.output_mcast_port = ToU16(argv[++i]);
        else if (arg == "--req-bind-ip" && i + 1 < argc) cfg.req_bind_ip = argv[++i];
        else if (arg == "--req-bind-port" && i + 1 < argc) cfg.req_bind_port = ToU16(argv[++i]);
        else if (arg == "--retrans-send-port" && i + 1 < argc) cfg.retrans_send_port = ToU16(argv[++i]);
        else if (arg == "--ring-capacity" && i + 1 < argc) cfg.ring_capacity = ToSize(argv[++i]);
        else if (arg == "--help")
        {
            std::cout
                << "Usage: patchServer [options]\n"
                << "  --input-mcast-ip <ip>\n"
                << "  --input-mcast-port <port>\n"
                << "  --output-mcast-ip <ip>\n"
                << "  --output-mcast-port <port>\n"
                << "  --req-bind-ip <ip>\n"
                << "  --req-bind-port <port>\n"
                << "  --retrans-send-port <port>\n"
                << "  --ring-capacity <num_packets>\n";
            return false;
        }
        else
        {
            std::cerr << "Unknown arg: " << arg << "\n";
            return false;
        }
    }
    return true;
}
}

int main(int argc, char* argv[])
{
    ServerConfig cfg;
    if (!ParseArgs(argc, argv, cfg))
    {
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
