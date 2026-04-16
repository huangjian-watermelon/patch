#include "retrans_server.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <iostream>
#include <vector>

#include "retrans_protocol.h"
#include "ts_packet.h"

namespace
{
    constexpr size_t kRecvBufSize = 1500;
    constexpr size_t kTsPacketSize = 188;
}

RetransServer::RetransServer(TsRingBuffer& ring_buffer, uint16_t retrans_send_port)
    : ring_buffer_(ring_buffer), retrans_send_port_(retrans_send_port)
{
}

RetransServer::~RetransServer()
{
    Stop();
}

bool RetransServer::Init(const std::string& bind_ip, uint16_t port)
{
    sockfd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0)
    {
        std::cerr << "[RetransServer] socket failed: "
                  << std::strerror(errno) << std::endl;
        return false;
    }

    int reuse = 1;
    if (::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        std::cerr << "[RetransServer] setsockopt SO_REUSEADDR failed: "
                  << std::strerror(errno) << std::endl;
        return false;
    }

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (bind_ip.empty() || bind_ip == "0.0.0.0")
    {
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
    }
    else
    {
        if (::inet_pton(AF_INET, bind_ip.c_str(), &addr.sin_addr) != 1)
        {
            std::cerr << "[RetransServer] inet_pton failed, ip="
                      << bind_ip << std::endl;
            return false;
        }
    }

    if (::bind(sockfd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
    {
        std::cerr << "[RetransServer] bind failed: "
                  << std::strerror(errno) << std::endl;
        return false;
    }

    std::cout << "[RetransServer] bind ok: "
              << bind_ip << ":" << port << std::endl;

    // ====== add: send sock (9001 send patch_packet) ======
    send_sockfd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (send_sockfd_ < 0)
    {
        perror("socket send");
        return false;
    }

    std::cout << "[RetransServer] send socket ready" << std::endl;

    return true;
}

void RetransServer::Run()
{
    running_ = true;

    uint8_t recv_buf[kRecvBufSize] = {0};

    while (running_)
    {
        sockaddr_in client_addr {};
        socklen_t client_len = sizeof(client_addr);

        const ssize_t n = ::recvfrom(sockfd_,
                                     recv_buf,
                                     sizeof(recv_buf),
                                     0,
                                     reinterpret_cast<sockaddr*>(&client_addr),
                                     &client_len);

        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            std::cerr << "[RetransServer] recvfrom failed: "
                      << std::strerror(errno) << std::endl;
            continue;
        }

        if (n == 0)
        {
            continue;
        }

        HandleRequest(recv_buf,
                      static_cast<size_t>(n),
                      client_addr,
                      client_len);
    }
}

void RetransServer::Stop()
{
    running_ = false;

    if (sockfd_ >= 0)
    {
        ::close(sockfd_);
        sockfd_ = -1;
    }

    if (send_sockfd_ >= 0)
    {
        ::close(send_sockfd_);
        send_sockfd_ = -1;
    }
}

void RetransServer::HandleRequest(const uint8_t* data,
                                  size_t len,
                                  const sockaddr_in& client_addr,
                                  socklen_t client_len)
{
    if (data == nullptr)
    {
        ++invalid_requests_;
        return;
    }

    if (len < sizeof(RetransHeader) + sizeof(RetransRequestBody))
    {
        ++invalid_requests_;
        std::cerr << "[RetransServer] invalid request len=" << len << std::endl;
        return;
    }

    const auto* header = reinterpret_cast<const RetransHeader*>(data);
    const uint16_t magic = ntohs(header->magic);
    const uint8_t version = header->version;
    const uint16_t msg_type = ntohs(header->msg_type);
    const uint16_t body_len = ntohs(header->body_len);
    const uint32_t request_id = ntohl(header->request_id);

    if (magic != RETRANS_MAGIC)
    {
        ++invalid_requests_;
        std::cerr << "[RetransServer] invalid magic=0x"
                  << std::hex << magic << std::dec << std::endl;
        return;
    }

    if (version != RETRANS_VERSION)
    {
        ++invalid_requests_;
        std::cerr << "[RetransServer] invalid version="
                  << static_cast<uint32_t>(version) << std::endl;
        return;
    }

    if (msg_type != static_cast<uint16_t>(RetransMsgType::REQUEST))
    {
        ++invalid_requests_;
        std::cerr << "[RetransServer] invalid msg_type="
                  << msg_type << std::endl;
        return;
    }

    if (body_len != sizeof(RetransRequestBody))
    {
        ++invalid_requests_;
        std::cerr << "[RetransServer] invalid body_len="
                  << body_len << std::endl;
        return;
    }

    const auto* req =
        reinterpret_cast<const RetransRequestBody*>(data + sizeof(RetransHeader));

    const uint64_t start_seq = NetToHost64(req->start_seq);
    const uint64_t request_session_id = NetToHost64(req->session_id);
    const uint16_t count = ntohs(req->count);

    if (count == 0)
    {
        ++invalid_requests_;
        std::cerr << "[RetransServer] request count=0, ignore" << std::endl;
        return;
    }

    ++recv_requests_;
    requested_packets_ += count;

    char client_ip[INET_ADDRSTRLEN] = {0};
    ::inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip));

    std::cout << "[RetransServer] request from "
              << client_ip << ":" << ntohs(client_addr.sin_port)
              << " request_id=" << request_id
              << " session_id=" << request_session_id
              << " start_seq=" << start_seq
              << " count=" << count << std::endl;

    for (uint16_t i = 0; i < count; ++i)
    {
        const uint64_t seq = start_seq + i;

        TsPacket packet;
        if (!ring_buffer_.GetBySeq(seq, packet))
        {
            ++miss_packets_;
            std::cout << "[RetransServer] seq not found, seq="
                      << seq << std::endl;
            continue;
        }

//        uint8_t send_buf[sizeof(RetransHeader) + sizeof(RetransResponseBody) + kTsPacketSize];

//        auto* rsp_header = reinterpret_cast<RetransHeader*>(send_buf);
//        rsp_header->magic = RETRANS_MAGIC;
//        rsp_header->msg_type = static_cast<uint16_t>(RetransMsgType::RESPONSE);
//        rsp_header->body_len = sizeof(RetransResponseBody) + kTsPacketSize;
//        rsp_header->reserved = 0;

//        auto* rsp_body = reinterpret_cast<RetransResponseBody*>(
//            send_buf + sizeof(RetransHeader));
//        rsp_body->seq = packet.seq;
//        rsp_body->data_len = kTsPacketSize;

//        std::memcpy(send_buf + sizeof(RetransHeader) + sizeof(RetransResponseBody),
//                    packet.data,
//                    kTsPacketSize);

        StreamPacket pkt{};
        pkt.session_id = request_session_id;
        pkt.seq = packet.seq;
        std::memcpy(pkt.ts_data, packet.data, kTsPacketSize);

        sockaddr_in client_send_addr = client_addr;
        client_send_addr.sin_port = htons(retrans_send_port_);
        const ssize_t sent = ::sendto(send_sockfd_,
                                      &pkt,
                                      sizeof(pkt),
                                      0,
                                      reinterpret_cast<const sockaddr*>(&client_send_addr),
                                      client_len);

        if (sent < 0)
        {
            std::cerr << "[RetransServer] sendto failed, seq=" << seq
                      << " err=" << std::strerror(errno) << std::endl;
            continue;
        }

        ++resent_packets_;
        std::cout << "[RetransServer] resend ok, seq=" << seq
                  << " bytes=" << sent << std::endl;
    }

    static uint64_t request_print_count = 0;
    ++request_print_count;
    if ((request_print_count % 100) == 0)
    {
        std::cout << "[RetransServer][STAT] recv_requests=" << recv_requests_
                  << " invalid_requests=" << invalid_requests_
                  << " requested_packets=" << requested_packets_
                  << " resent_packets=" << resent_packets_
                  << " ring_miss_packets=" << miss_packets_
                  << std::endl;
    }
}
