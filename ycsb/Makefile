#
#  Makefile
#  YCSB-cpp
#
#  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
#  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
#


#---------------------build config-------------------------

DEBUG_BUILD ?= 0
EXTRA_CXXFLAGS ?=
EXTRA_LDFLAGS ?=
MOONKV ?= 0
MATRIXKV ?= 0
ROCKSDB ?= 0

BIND_LEVELDB ?= 0
BIND_ROCKSDB ?= 0
BIND_TITANDB ?= 0
BIND_LMDB ?= 0

#----------------------------------------------------------
ifeq ($(MOONKV), 1)
	SOURCES += $(wildcard rocksdb/*.cc)	
	CXXFLAGS += -Dnvm_rocksdb
	CXXFLAGS += -I ../include 
	LDFLAGS += -L ../build_release 
	LDFLAGS += -lrocksdb -lsnappy -lpmem -lfolly -lgflags -lglog -lfmt
endif
ifeq ($(MATRIXKV), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
	CXXFLAGS += -Dmatrixkv
#note: next line should use Matrixkv's "include folder" path, xxx is an example
	CXXFLAGS += -I/xxx/include 
#note: next line should use MatrixKV's link library path, xxx is an example
endif

ifeq ($(ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
#note: next line should use RocksDB's "include folder" path, xxx is an example
	CXXFLAGS += -I/xxx/include 
#note: next line should use RocksDB's link library path, xxx is an example
	LDFLAGS += -L/xxx 
endif

ifeq ($(DEBUG_BUILD), 1)
	CXXFLAGS += -g
else
	CXXFLAGS += -O2
	CPPFLAGS += -DNDEBUG
endif

ifeq ($(BIND_LEVELDB), 1)
	LDFLAGS += -lleveldb
	SOURCES += $(wildcard leveldb/*.cc)
endif

ifeq ($(BIND_ROCKSDB), 1)
	LDFLAGS += -lrocksdb
	SOURCES += $(wildcard rocksdb/*.cc)
endif

ifeq ($(BIND_LMDB), 1)
	LDFLAGS += -llmdb
	SOURCES += $(wildcard lmdb/*.cc)
endif

CXXFLAGS += -std=c++17 -Wall -pthread $(EXTRA_CXXFLAGS) -I./
LDFLAGS += $(EXTRA_LDFLAGS) -lpthread
SOURCES += $(wildcard core/*.cc)
OBJECTS += $(SOURCES:.cc=.o)
DEPS += $(SOURCES:.cc=.d)
ifeq ($(MOONKV), 1)
	EXEC = ycsb_moonkv
else 
	ifeq ($(MATRIXKV), 1)
		EXEC = ycsb_matrixkv
	else
		EXEC = ycsb_rocksdb
	endif
endif

all: $(EXEC)

$(EXEC): $(OBJECTS)
	@$(CXX) $(CXXFLAGS) $^ $(LDFLAGS) -o $@
	@echo "  LD      " $@

.cc.o:
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -c -o $@ $<
	@echo "  CC      " $@

%.d : %.cc
	@$(CXX) $(CXXFLAGS) $(CPPFLAGS) -MM -MT '$(<:.cc=.o)' -o $@ $<

ifneq ($(MAKECMDGOALS),clean)
-include $(DEPS)
endif

clean:
	find . -name "*.[od]" -delete

.PHONY: clean
