TEMPLATE = app
CONFIG += console c++11 thread
CONFIG -= app_bundle
CONFIG -= qt

TARGET = client

SOURCES += \
        ../shared/json_config.cpp \
        main.cpp \
        packet_reorder_buffer.cpp \
        retrans_request_manager.cpp \
        ts_output_sender.cpp

HEADERS += \
    retrans_protocol.h \
    recv_stream.h \
    packet_reorder_buffer.h \
    ts_output_sender.h \
    retrans_request_manager.h
