#include <iostream>
#include <thread>
#include "udp_ts_receive.h"
#include "ts_ring_buffer.h"
#include "retrans_server.h"

using namespace std;

namespace  {
    constexpr uint16_t RETRANS_REQUEST_RECV_PORT = 9000; // 收补包，建议单独端口
    constexpr uint16_t RETRANS_SEND_PORT = 9001;  // 发补包请求到服务端
}

int main()
{
    TsRingBuffer ringBuffer(100 * 1024);

    UdpTsReceiver receiver(ringBuffer);
    RetransServer retrans_server(ringBuffer);

    if (!receiver.Init("238.1.1.130", 1234))
    {
        std::cerr << "Receiver init failed!\n";
        return 1;
    }

    if (!receiver.InitSend("238.1.1.127", 5040))
    {
        std::cerr << "Send init failed!\n";
        return 1;
    }

    if (!retrans_server.Init("0.0.0.0", RETRANS_REQUEST_RECV_PORT))
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
