QZMQ_DIR = $$PWD/../qzmq
COMMON_DIR = $$PWD/../common

INCLUDEPATH += $$QZMQ_DIR/src
include($$QZMQ_DIR/src/src.pri)

INCLUDEPATH += $$COMMON_DIR

DEFINES += NO_IRISNET

HEADERS += \
	$$COMMON_DIR/processquit.h \
	$$COMMON_DIR/tnetstring.h \
	$$COMMON_DIR/httpheaders.h \
	$$COMMON_DIR/bufferlist.h \
	$$COMMON_DIR/log.h

SOURCES += \
	$$COMMON_DIR/processquit.cpp \
	$$COMMON_DIR/tnetstring.cpp \
	$$COMMON_DIR/httpheaders.cpp \
	$$COMMON_DIR/bufferlist.cpp \
	$$COMMON_DIR/log.cpp

HEADERS += \
	$$PWD/httprequest.h \
	$$PWD/client.h \
	$$PWD/clientthread.h \
	$$PWD/app.h

SOURCES += \
	$$PWD/httprequest.cpp \
	$$PWD/client.cpp \
	$$PWD/clientthread.cpp \
	$$PWD/app.cpp \
	$$PWD/main.cpp
