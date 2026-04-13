#pragma once
#include <atomic>
#include <string>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "ts_ring_buffer.h"


struct StreamPacket
{
    uint64_t seq;
    uint8_t ts_data[188];
};


class RetransServer
{
public:
    explicit RetransServer(TsRingBuffer& ring_buffer);
    ~RetransServer();

    bool Init(const std::string& bind_ip, uint16_t port);
    void Run();
    void Stop();

private:
    void HandleRequest(const uint8_t* data, size_t len,
                       const sockaddr_in& client_addr, socklen_t client_len);

private:
    int sockfd_ = -1;
    int send_sockfd_ = -1;
    std::atomic<bool> running_{false};
    TsRingBuffer& ring_buffer_;
};
