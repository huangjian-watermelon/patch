#include <iostream>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <string>
#include <thread>
#include <csignal>

#include "../shared/json_config.h"
#include "retrans_server.h"
#include "retrans_protocol.h"
#include "stream_packet.h"
#include "ts_ring_buffer.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {
struct RetransServiceConfig
{
    std::string input_mcast_ip = "238.1.1.127";
    uint16_t input_mcast_port = 5040;

    std::string req_bind_ip = "0.0.0.0";
    uint16_t req_bind_port = 9000;
    size_t req_rcvbuf_bytes = 4 * 1024 * 1024;
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
    json.GetSize("req_rcvbuf_bytes", cfg.req_rcvbuf_bytes);
    json.GetUInt16("retrans_send_port", cfg.retrans_send_port);
    json.GetSize("ring_capacity", cfg.ring_capacity);
    return true;
}

uint64_t GetNowUs()
{
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(now).count());
}

void IgnoreTerminalSignals()
{
    std::signal(SIGINT, SIG_IGN);
    std::signal(SIGHUP, SIG_IGN);
}

bool InitStreamPacketSocket(const RetransServiceConfig& cfg, int& sockfd)
{
    sockfd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0)
    {
        std::cerr << "[RetransService] create stream socket failed\n";
        return false;
    }

    int reuse = 1;
    ::setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    int rcvbuf = 4 * 1024 * 1024;
    ::setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    sockaddr_in local_addr{};
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons(cfg.input_mcast_port);
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(sockfd, reinterpret_cast<sockaddr*>(&local_addr), sizeof(local_addr)) < 0)
    {
        std::cerr << "[RetransService] bind stream socket failed\n";
        return false;
    }

    ip_mreq mreq{};
    ::inet_pton(AF_INET, cfg.input_mcast_ip.c_str(), &mreq.imr_multiaddr);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);

    if (::setsockopt(sockfd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
    {
        std::cerr << "[RetransService] IP_ADD_MEMBERSHIP failed: " << strerror(errno) << '\n';
        return false;
    }

    std::cout << "[RetransService] receive forward stream from "
              << cfg.input_mcast_ip << ":" << cfg.input_mcast_port << std::endl;
    return true;
}

void StreamPacketRecvLoop(int sockfd, TsRingBuffer& ring_buffer)
{
    StreamPacket pkt{};
    auto last_warn = std::chrono::steady_clock::time_point{};
    while (true)
    {
        const ssize_t n = ::recvfrom(sockfd, &pkt, sizeof(pkt), 0, nullptr, nullptr);
        if (n != static_cast<ssize_t>(sizeof(pkt)))
        {
            auto now = std::chrono::steady_clock::now();
            if (last_warn.time_since_epoch().count() == 0 ||
                std::chrono::duration_cast<std::chrono::seconds>(now - last_warn).count() >= 1)
            {
                std::cerr << "[RetransService] unexpected packet size " << n
                          << ", expect " << sizeof(pkt)
                          << " (check input_mcast_ip/input_mcast_port, should be StreamPacket flow)"
                          << std::endl;
                last_warn = now;
            }
            continue;
        }

        if (pkt.ts_data[0] != 0x47)
        {
            continue;
        }

        TsPacket packet;
        packet.session_id = NetToHost64(pkt.session_id);
        packet.seq = NetToHost64(pkt.seq);
        packet.recv_time_us = GetNowUs();
        std::memcpy(packet.data, pkt.ts_data, TS_PACKET_SIZE);

        ring_buffer.Push(packet);
    }
}
}

int main(int argc, char* argv[])
{
    IgnoreTerminalSignals();

    RetransServiceConfig cfg;
    const std::string config_path = (argc >= 2) ? argv[1] : "retrans_service.json";
    if (!LoadConfig(config_path, cfg))
    {
        std::cerr << "Usage: patchRetransServer <config.json>\n";
        return 1;
    }

    TsRingBuffer ring_buffer(cfg.ring_capacity);
    RetransServer retrans_server(ring_buffer, cfg.retrans_send_port);
    int stream_sock = -1;

    if (!InitStreamPacketSocket(cfg, stream_sock))
    {
        std::cerr << "Stream packet receiver init failed!\n";
        return 1;
    }

    if (!retrans_server.Init(cfg.req_bind_ip,
                             cfg.req_bind_port,
                             static_cast<int>(cfg.req_rcvbuf_bytes)))
    {
        std::cerr << "RetransServer init failed!\n";
        return 1;
    }

    std::thread t1([&]() { StreamPacketRecvLoop(stream_sock, ring_buffer); });
    std::thread t2([&]() { retrans_server.Run(); });

    t1.join();
    t2.join();
    return 0;
}
