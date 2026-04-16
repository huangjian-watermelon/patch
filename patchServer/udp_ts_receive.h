#pragma once
#include <atomic>
#include <string>
#include <cstdint>

#include <arpa/inet.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "ts_ring_buffer.h"
#include "ts_loss_detector.h"

class UdpTsReceiver
{
public:
    explicit UdpTsReceiver(TsRingBuffer& ring_buffer);
    ~UdpTsReceiver();

    bool Init(const std::string& ip, uint16_t port);
    bool InitSend(const std::string& ip, uint16_t port);
    void Run();
    void Stop();

private:
    void HandleUdpPacket(const uint8_t* data, size_t len);
    uint64_t GetNowUs() const;
    uint64_t GetNowMs() const;
    uint64_t CreateSessionId() const;

private:
    int sockfd_ = -1;
    std::atomic<bool> running_{false};

    int sendsockfd_ = -1;
    bool has_client_ = false;
    sockaddr_in client_addr_ {};

    TsRingBuffer& ring_buffer_;
    uint64_t session_id_ = 0;
    uint64_t global_seq_ = 0;
    TsLossDetector loss_detector_;

    uint64_t udp_packet_count_ = 0;
    uint64_t ts_packet_count_ = 0;
    uint64_t invalid_packet_count_ = 0;
};
