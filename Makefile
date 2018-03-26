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
STRESS=r3c_stress

HIREDIS?=/usr/local/hiredis
PREFIX?=/usr/local
INCLUDE_PATH?=include/r3c
LIBRARY_PATH?=lib

# redis-cluster configuration used for testing
REDIS_CLUSTER_NODES?=192.168.1.31:6379,192.168.1.31:6380

INSTALL_BIN=$(PREFIX)/bin
INSTALL_INCLUDE_PATH= $(PREFIX)/$(INCLUDE_PATH)
INSTALL_LIBRARY_PATH= $(PREFIX)/$(LIBRARY_PATH)
INSTALL?= cp -a

#OPTIMIZATION?=-O3
DEBUG?= -g -ggdb # -DSLEEP_USE_POLL=1
WARNINGS=-Wall -W -Wwrite-strings
REAL_CPPFLAGS=$(CPPFLAGS) $(ARCH) -I$(HIREDIS)/include -D__STDC_FORMAT_MACROS=1 -fstrict-aliasing -fPIC $(DEBUG) $(OPTIMIZATION) $(WARNINGS)
REAL_LDFLAGS=$(LDFLAGS) $(ARCH) $(HIREDIS)/lib/libhiredis.a

CXX:=$(shell sh -c 'type $(CXX) >/dev/null 2>/dev/null && echo $(CXX) || echo g++')
STLIBSUFFIX=a
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)
STLIB_MAKE_CMD=ar rcs

all: $(STLIBNAME) $(CMD) $(TEST) $(STRESS)

# Deps (use make dep to generate this)
sha1.o: sha1.cpp
utils.o: utils.cpp utils.h
r3c.o: r3c.cpp r3c.h
r3c_cmd.o: r3c_cmd.cpp r3c.h utils.cpp
r3c_test.o: r3c_test.cpp r3c.h utils.cpp
r3c_stress.o: r3c_stress.cpp r3c.h utils.cpp

%.o: %.cpp
	$(CXX) -c $< $(REAL_CPPFLAGS)

$(STLIBNAME): sha1.o utils.o r3c.o
	rm -f $@;$(STLIB_MAKE_CMD) $@ $^

$(CMD): r3c_cmd.o $(STLIBNAME)
	$(CXX) -o $@ $^ $(REAL_LDFLAGS)

$(TEST): r3c_test.o $(STLIBNAME)
	$(CXX) -o $@ $^ $(REAL_LDFLAGS)

$(STRESS): r3c_stress.o $(STLIBNAME)
	$(CXX) -o $@ $^ $(REAL_LDFLAGS)
	
clean:
	rm -f $(STLIBNAME) $(CMD) $(TEST) $(STRESS) *.o core core.*

install:
	mkdir -p $(INSTALL_BIN)
	mkdir -p $(INSTALL_INCLUDE_PATH) $(INSTALL_LIBRARY_PATH)
	$(INSTALL) r3c.h $(INSTALL_INCLUDE_PATH)
	$(INSTALL) $(STLIBNAME) $(INSTALL_LIBRARY_PATH)
	$(INSTALL) $(CMD) $(INSTALL_BIN)

dep:
	$(CXX) -MM *.cpp

test: all
	./r3c_test $(REDIS_CLUSTER_NODES)
