#include <atomic>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <chrono>
#include <string>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../shared/json_config.h"
#include "recv_stream.h"
#include "retrans_protocol.h"
#include "packet_reorder_buffer.h"
#include "ts_output_sender.h"
#include "retrans_request_manager.h"

using namespace std;

namespace
{
struct ClientConfig
{
    std::string stream_mcast_ip = "238.1.1.127";
    uint16_t stream_port = 5040;
    std::string server_ip = "127.0.0.1";
    uint16_t retrans_request_port = 9000;
    uint16_t retrans_recv_port = 9001;
    std::string output_mcast_ip = "238.1.1.136";
    uint16_t output_mcast_port = 1234;
};

bool LoadConfig(const std::string& path, ClientConfig& cfg)
{
    JsonConfig json;
    std::string err;
    if (!JsonConfig::LoadFromFile(path, json, err))
    {
        std::cerr << "Load config failed: " << err << "\n";
        return false;
    }

    json.GetString("stream_mcast_ip", cfg.stream_mcast_ip);
    json.GetUInt16("stream_port", cfg.stream_port);
    json.GetString("server_ip", cfg.server_ip);
    json.GetUInt16("retrans_request_port", cfg.retrans_request_port);
    json.GetUInt16("retrans_recv_port", cfg.retrans_recv_port);
    json.GetString("output_mcast_ip", cfg.output_mcast_ip);
    json.GetUInt16("output_mcast_port", cfg.output_mcast_port);

    return true;
}
}

static PacketReorderBuffer g_reorder_buffer;
static RetransRequestManager g_retrans_mgr;
static std::atomic<bool> g_running{true};
static std::atomic<uint64_t> g_current_session_id{0};

void MainStreamLoop(int stream_sock, int req_sock, const sockaddr_in& server_addr)
{
    uint64_t last_seq = 0;
    bool has_last = false;

    StreamPacket pkt{};

    while (g_running)
    {
        ssize_t n = ::recvfrom(stream_sock,
                               &pkt,
                               sizeof(pkt),
                               0,
                               nullptr,
                               nullptr);

        if (n != static_cast<ssize_t>(sizeof(pkt)))
        {
            continue;
        }

        if (pkt.ts_data[0] != 0x47)
        {
            continue;
        }

        uint64_t seq = pkt.seq;
        const uint64_t pkt_session_id = pkt.session_id;

        const uint64_t current_session_id = g_current_session_id.load(std::memory_order_relaxed);
        if (current_session_id == 0 || pkt_session_id != current_session_id)
        {
            std::cout << "[main stream] switch session_id from " << current_session_id
                      << " to " << pkt_session_id << ", reset state" << std::endl;

            g_current_session_id.store(pkt_session_id, std::memory_order_relaxed);
            g_retrans_mgr.OnSessionChanged(pkt_session_id);
            g_reorder_buffer.ResetForNewSession(pkt_session_id, pkt.seq);
            has_last = false;
        }

        if (has_last && seq > last_seq + 1)
        {
            uint64_t miss_start = last_seq + 1;
            uint16_t miss_count = static_cast<uint16_t>(seq - last_seq - 1);

            std::cout << "[main stream] missing start_seq = " << miss_start
                      << " count = " << miss_count << std::endl;

            g_retrans_mgr.OnMissingRange(miss_start, miss_count);
        }

        has_last = true;
        last_seq = seq;

        g_retrans_mgr.OnPacketRecovered(pkt.seq);
        g_reorder_buffer.OnPacket(pkt);
    }
    std::cout << "MainStreamThread is exit!" << std::endl;
}

void RetransRecvLoop(int retrans_recv_sock)
{
    StreamPacket pkt{};

    while (g_running)
    {
        ssize_t n = ::recvfrom(retrans_recv_sock,
                               &pkt,
                               sizeof(pkt),
                               0,
                               nullptr,
                               nullptr);

        if (n != static_cast<ssize_t>(sizeof(pkt)))
        {
            continue;
        }

        if (pkt.ts_data[0] != 0x47)
        {
            continue;
        }

        const uint64_t current_session_id = g_current_session_id.load(std::memory_order_relaxed);
        if (current_session_id != 0 && pkt.session_id != current_session_id)
        {
            continue;
        }

        g_retrans_mgr.OnPacketRecovered(pkt.seq);
        g_reorder_buffer.OnPacket(pkt);
    }

    std::cout << "RetransRecvLoop is exit!" << std::endl;

}

void RetransTimeoutLoop()
{
    while (g_running)
    {
        std::vector<uint64_t> expired_seqs;
        g_retrans_mgr.CheckTimeouts(expired_seqs);

        for (uint64_t seq : expired_seqs)
        {
            std::cout << "[timeout] skip seq=" << seq << std::endl;

            // 关键：通知重排缓冲区，这个包别再等了，直接跳过
            g_reorder_buffer.MarkSeqExpired(seq);
        }

        g_retrans_mgr.Cleanup();

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::cout << "RetransTimeoutLoop is exit!" << std::endl;

}

void StatLoop()
{
    while (g_running)
    {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        g_reorder_buffer.PrintStats();
        g_retrans_mgr.PrintStats();
    }
}

int main(int argc, char* argv[])
{
    ClientConfig cfg;
    const std::string config_path = (argc >= 2) ? argv[1] : "ts_request_client.json";
    if (!LoadConfig(config_path, cfg))
    {
        std::cerr << "Usage: ts_request_client <config.json>\n";
        return 1;
    }

    if (!g_reorder_buffer.InitDeliver(cfg.output_mcast_ip, cfg.output_mcast_port))
    {
        std::cerr << "InitDeliver failed!\n";
        return 1;
    }

    // ===== 1. 主流接收 socket =====
    int stream_sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (stream_sock < 0)
    {
        std::cerr << "create stream socket failed\n";
        return 1;
    }

    int reuse = 1;
    ::setsockopt(stream_sock, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    int rcvbuf = 4 * 1024 * 1024;
    ::setsockopt(stream_sock, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    sockaddr_in local_addr{};
    local_addr.sin_family = AF_INET;
    local_addr.sin_port = htons(cfg.stream_port);
    local_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(stream_sock, reinterpret_cast<sockaddr*>(&local_addr), sizeof(local_addr)) < 0)
    {
        std::cerr << "bind stream socket failed\n";
        return 1;
    }

    ip_mreq mreq{};
    ::inet_pton(AF_INET, cfg.stream_mcast_ip.c_str(), &mreq.imr_multiaddr);
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);

    if (::setsockopt(stream_sock, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
    {
        std::cerr << "IP_ADD_MEMBERSHIP failed: " << strerror(errno) << '\n';
        return 1;
    }

    // ===== 2. 补包请求 socket（发送用）=====
    int req_sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (req_sock < 0)
    {
        std::cerr << "create req socket failed\n";
        return 1;
    }

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(cfg.retrans_request_port);
    ::inet_pton(AF_INET, cfg.server_ip.c_str(), &server_addr.sin_addr);

    g_retrans_mgr.Init(req_sock, server_addr);

    // ===== 3. 补包接收 socket =====
    int retrans_recv_sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (retrans_recv_sock < 0)
    {
        std::cerr << "create retrans recv socket failed\n";
        return 1;
    }

    sockaddr_in retrans_local_addr{};
    retrans_local_addr.sin_family = AF_INET;
    retrans_local_addr.sin_port = htons(cfg.retrans_recv_port);
    retrans_local_addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(retrans_recv_sock,
               reinterpret_cast<sockaddr*>(&retrans_local_addr),
               sizeof(retrans_local_addr)) < 0)
    {
        std::cerr << "bind retrans recv socket failed\n";
        return 1;
    }

    // ===== 4.=====
    TsOutputSender output_sender;
    if (!output_sender.Init(cfg.output_mcast_ip, cfg.output_mcast_port))
    {
        std::cerr << "output sender init failed\n";
        return 1;
    }
    output_sender.Start();

    g_reorder_buffer.SetSender(&output_sender);

    std::thread t1(MainStreamLoop, stream_sock, req_sock, server_addr);
    std::thread t2(RetransRecvLoop, retrans_recv_sock);
    std::thread t3(StatLoop);
    std::thread t4(RetransTimeoutLoop);

    t1.join();
    t2.join();
    t3.join();
    t4.join();

    return 0;
}
