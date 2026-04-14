#include "udp_ts_receive.h"
#include "ts_parsed_header.h"
#include "stream_packet.h"

#include <arpa/inet.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <cstring>
#include <iostream>
#include <chrono>

UdpTsReceiver::UdpTsReceiver(TsRingBuffer& ring_buffer)
    : ring_buffer_(ring_buffer)
{
}

UdpTsReceiver::~UdpTsReceiver()
{
    Stop();
}

bool UdpTsReceiver::Init(const std::string& ip, uint16_t port)
{
    sockfd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd_ < 0)
    {
        std::cerr << "socket failed\n";
        return false;
    }

    int reuse = 1;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    int rcvbuf = 4 * 1024 * 1024;
    ::setsockopt(sockfd_, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(sockfd_, (sockaddr*)&addr, sizeof(addr)) < 0)
    {
        std::cerr << "bind failed\n";
        return false;
    }

    ip_mreq mreq {};
    mreq.imr_multiaddr.s_addr = inet_addr(ip.c_str());
    mreq.imr_interface.s_addr = htonl(INADDR_ANY);
    if(::setsockopt(sockfd_, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0) {
        std::cerr << "IP_ADD_MEMBERSHIP failed: " << strerror(errno) << std::endl;
        return false;
    }

    std::cout << "join multicast group " << ip << ":" << port << std::endl;

    return true;
}

bool UdpTsReceiver::InitSend(const std::string &ip, uint16_t port)
{
    sendsockfd_ = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (sendsockfd_ < 0)
    {
        std::cerr << "socket failed\n";
        return false;
    }

    int reuse = 1;
    ::setsockopt(sendsockfd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    int ttl = 16;
    ::setsockopt(sendsockfd_, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

    in_addr local_if {};
    local_if.s_addr = htonl(INADDR_ANY);
    ::setsockopt(sendsockfd_, IPPROTO_IP, IP_MULTICAST_IF, &local_if, sizeof(local_if));

    client_addr_.sin_family = AF_INET;
    client_addr_.sin_port = htons(port);
    inet_pton(AF_INET, ip.c_str(), &client_addr_.sin_addr);

    std::cout << "udpStream create success!" << std::endl;
    has_client_ = true;
    return true;
}

void UdpTsReceiver::Run()
{
    running_ = true;

    uint8_t buf[2048] = {0};

    while (running_)
    {
        ssize_t n = ::recvfrom(sockfd_, buf, sizeof(buf), 0, nullptr, nullptr);

        if (n <= 0)
        {
            continue;
        }

        ++udp_packet_count_;

        HandleUdpPacket(buf, static_cast<size_t>(n));

        if ((udp_packet_count_ % 102400) == 0)
        {
            std::cout
                << "[STAT] udp=" << udp_packet_count_
                << " ts=" << ts_packet_count_
                << " invalid=" << invalid_packet_count_
                << " seq_range=[" << ring_buffer_.GetMinSeq()
                << "," << ring_buffer_.GetMaxSeq() << "]"
                << std::endl;
        }
    }
}

void UdpTsReceiver::Stop()
{
    running_ = false;

    if (sockfd_ >= 0)
    {
        ::close(sockfd_);
        sockfd_ = -1;
    }

    if (sendsockfd_ >= 0)
    {
        ::close(sendsockfd_);
        sendsockfd_ = -1;
    }
}

void UdpTsReceiver::HandleUdpPacket(const uint8_t* data, size_t len)
{
    if (len % TS_PACKET_SIZE != 0)
    {
        std::cout << "len % TS_PACKET_SIZE" << std::endl;
        ++invalid_packet_count_;
        return;
    }

    for (size_t offset = 0; offset + TS_PACKET_SIZE <= len; offset += TS_PACKET_SIZE)
    {
        if (data[offset] != 0x47)
        {
            std::cout << "data[offset] != 0x47" << std::endl;
            ++invalid_packet_count_;
            continue;
        }

        TsPacket packet;
        packet.seq = global_seq_++;
        packet.recv_time_us = GetNowUs();
        std::memcpy(packet.data, data + offset, TS_PACKET_SIZE);

        TsParsedHeader header;
        if (ParseTsHeader(packet.data, header))
        {
            loss_detector_.OnPacket(packet.seq, header);
        }
        else
        {
            std::cout << "[ERROR] invalid TS header, seq = " << packet.seq << std::endl;
        }

        static auto last_print_time = GetNowMs();
        if (GetNowMs() - last_print_time >= 10000)
        {
            loss_detector_.PrintStats();
            last_print_time = GetNowMs();
        }

        ring_buffer_.Push(packet);

        if (has_client_)
        {
//            if (packet.seq % 100 == 0)
//                continue;
            StreamPacket pkt{};
            pkt.seq = packet.seq;
            std::memcpy(pkt.ts_data, packet.data, TS_PACKET_SIZE);

            const ssize_t sent = ::sendto(sendsockfd_,
                     &pkt,
                     sizeof(pkt),
                     0,
                     reinterpret_cast<const sockaddr*>(&client_addr_),
                     sizeof(client_addr_));

            if (sent < 0)
            {
                std::cerr << "[sendUDP] sendto failed"
                          << " err=" << std::strerror(errno) << std::endl;
                continue;
            }
        }

        ++ts_packet_count_;
    }
}

uint64_t UdpTsReceiver::GetNowUs() const
{
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::microseconds>(now).count();
}

uint64_t UdpTsReceiver::GetNowMs() const
{
    auto now = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now).count();
}
