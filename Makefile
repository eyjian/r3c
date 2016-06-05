# Redis Cluster Client Makefile
#
# Copyright (C) 2016 Jian Yi <eyjian at gmail dot com>
# This file is released under the BSD license, see the COPYING file
#
# Dependencies are stored in the Makefile.dep file. To rebuild this file
# Just use 'make dep > Makefile.dep', but this is only needed by developers.

LIBNAME=libr3c
CMD=r3c_cmd
TEST=r3c_test

HIREDIS?=/usr/local/hiredis
PREFIX?=/usr/local
INCLUDE_PATH?=include/r3c
LIBRARY_PATH?=lib

# redis-cluster configuration used for testing
REDIS_CLUSTER_NODES?=10.212.2.171:6379,10.212.2.171:6380

INSTALL_BIN=$(PREFIX)/bin
INSTALL_INCLUDE_PATH= $(PREFIX)/$(INCLUDE_PATH)
INSTALL_LIBRARY_PATH= $(PREFIX)/$(LIBRARY_PATH)
INSTALL?= cp -a

R3C_GCC?=g++
R3C_AR?=ar cr

CPPFLAGS=-I$(HIREDIS)/include -g -Wall -D__STDC_FORMAT_MACROS=1
LDFLAGS=$(HIREDIS)/lib/libhiredis.a -g

STLIBSUFFIX=a
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)

all: $(STLIBNAME) $(CMD) $(TEST)

# Deps (use make dep to generate this)
crc16.o: crc16.cpp
r3c.o: r3c.cpp r3c.h
r3c_cmd.o: r3c_cmd.cpp r3c.h
r3c_test.o: r3c_test.cpp r3c.h

%.o: %.cpp
	$(R3C_GCC) -c $< $(CPPFLAGS)

$(STLIBNAME): crc16.o r3c.o
	rm -f $@;$(R3C_AR) $@ $^

$(CMD): r3c_cmd.o $(STLIBNAME)
	$(R3C_GCC) -o $@ $^ $(LDFLAGS)

$(TEST): r3c_test.o $(STLIBNAME)
	$(R3C_GCC) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(STLIBNAME) $(CMD) $(TEST) *.o core core.*

install:
	mkdir -p $(INSTALL_INCLUDE_PATH) $(INSTALL_LIBRARY_PATH)
	$(INSTALL) r3c.h $(INSTALL_INCLUDE_PATH)
	$(INSTALL) $(STLIBNAME) $(INSTALL_LIBRARY_PATH)
	$(INSTALL) $(CMD) $(INSTALL_BIN)

dep:
	$(R3C_GCC) -MM *.cpp

test: all
	./r3c_test $(REDIS_CLUSTER_NODES)
