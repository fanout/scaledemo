CONFIG += console
CONFIG -= app_bundle
QT -= gui
QT += network
TARGET = clientmanager

MOC_DIR = $$OUT_PWD/_moc
OBJECTS_DIR = $$OUT_PWD/_obj

include($$OUT_PWD/conf.pri)
include(clientmanager.pri)
