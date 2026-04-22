TEMPLATE = app
CONFIG += console c++11 thread
CONFIG -= app_bundle
CONFIG -= qt
TARGET = retrans

SOURCES += \
    ../shared/json_config.cpp \
    retrans_service_main.cpp \
    ts_loss_detector.cpp \
    ts_ring_buffer.cpp \
    udp_ts_receive.cpp \
    retrans_server.cpp

HEADERS += \
    ts_ring_buffer.h \
    ts_loss_detector.h \
    ts_packet.h \
    ts_parsed_header.h \
    udp_ts_receive.h \
    retrans_protocol.h \
    retrans_server.h \
    stream_packet.h
