TEMPLATE = app
CONFIG += console c++11 thread
CONFIG -= app_bundle
CONFIG -= qt

TARGET = tsRequestClient

SOURCES += \
        main.cpp

HEADERS += \
    retrans_protocol.h \
    recv_stream.h \
    packet_reorder_buffer.h \
    ts_output_sender.h \
    retrans_request_manager.h
