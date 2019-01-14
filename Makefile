# Writed by yijian (eyjian@gmail.com)
# Redis Cluster Client Makefile
#
#
# Dependencies are stored in the Makefile.dep file. To rebuild this file
# Just use 'make dep > Makefile.dep', but this is only needed by developers.

LIBNAME=libr3c
CMD=r3c_cmd
TEST=r3c_test
STRESS=r3c_stress
ROBUST=r3c_robust
STREAM=r3c_stream
EXTENSION=redis_command_extension.so

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

#GCC_VERSION=$(shell gcc --version|awk -F[\ .]+ '/GCC/{printf("%s%s\n",$$3,$$4);}')
CPLUSPLUSONEONE=$(shell gcc --version|awk -F[\ .]+ '/GCC/{if($$3>=4&&$$4>=7) printf("-std=c++11");}')

#OPTIMIZATION?=-O2
DEBUG?= -g -ggdb $(CPLUSPLUSONEONE) # -DSLEEP_USE_POLL=1
WARNINGS=-Wall -W -Wwrite-strings -Wno-missing-field-initializers
REAL_CPPFLAGS=$(CPPFLAGS) $(ARCH) -I$(HIREDIS)/include -DSLEEP_USE_POLL=1 -D__STDC_FORMAT_MACROS=1 -D__STDC_CONSTANT_MACROS -fstrict-aliasing -fPIC  -pthread $(DEBUG) $(OPTIMIZATION) $(WARNINGS)
REAL_LDFLAGS=$(LDFLAGS) $(ARCH) -fPIC -pthread $(HIREDIS)/lib/libhiredis.a

CXX:=$(shell sh -c 'type $(CXX) >/dev/null 2>/dev/null && echo $(CXX) || echo g++')
STLIBSUFFIX=a
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)
STLIB_MAKE_CMD=ar rcs

all: $(STLIBNAME) $(CMD) $(TEST) $(STRESS) $(ROBUST) $(STREAM) $(EXTENSION)

# Deps (use make dep to generate this)
sha1.o: sha1.cpp
utils.o: utils.h utils.cpp
r3c.o: r3c.cpp r3c.h r3c.cpp utils.h utils.cpp
r3c_cmd.o: r3c_cmd.cpp r3c.h r3c.cpp utils.h utils.cpp
r3c_test.o: r3c_test.cpp r3c.h r3c.cpp utils.h utils.cpp
r3c_stress.o: r3c_stress.cpp r3c.h r3c.cpp utils.cpp
r3c_test.o: r3c_test.cpp r3c.h r3c.cpp utils.h utils.cpp
r3c_robust.o: r3c_robust.cpp r3c.h r3c.cpp utils.h utils.cpp
r3c_stream.o: r3c_stream.cpp r3c.h r3c.cpp utils.h utils.cpp
redis_command_extension.o: redis_command_extension.cpp r3c.h r3c.cpp utils.h utils.cpp

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

$(ROBUST): r3c_robust.o $(STLIBNAME)
	$(CXX) -o $@ $^ $(REAL_LDFLAGS) -pthread

$(STREAM): r3c_stream.o $(STLIBNAME)
	$(CXX) -o $@ $^ $(REAL_LDFLAGS) -pthread

$(EXTENSION): redis_command_extension.o $(STLIBNAME)
	$(CXX) -o $@ -shared $^ $(REAL_LDFLAGS)

clean:
	rm -f $(STLIBNAME) $(CMD) $(TEST) $(STRESS) $(ROBUST) $(STREAM) $(EXTENSION) *.o core core.*

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
