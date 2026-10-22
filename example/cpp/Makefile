CXX ?= c++
PKG_CONFIG ?= pkg-config
HIREDIS_PREFIX := $(shell brew --prefix hiredis 2>/dev/null)

ifeq ($(shell $(PKG_CONFIG) --exists hiredis 2>/dev/null; echo $$?),0)
  CXXFLAGS += $(shell $(PKG_CONFIG) --cflags hiredis)
  LDLIBS += $(shell $(PKG_CONFIG) --libs hiredis)
else
  CXXFLAGS += -I$(HIREDIS_PREFIX)/include
  LDLIBS += -L$(HIREDIS_PREFIX)/lib -lhiredis
endif

CXXFLAGS += -Wall -Wextra -std=c++17

SRCS := string.cpp list.cpp hash.cpp set.cpp zset.cpp hyperloglog.cpp slowlog.cpp dataframe.cpp pubsub.cpp
BINS := $(SRCS:%.cpp=bin/%)

.PHONY: all clean
all: $(BINS)

bin/%: %.cpp redisx_client.hpp | bin
	$(CXX) $(CXXFLAGS) -o $@ $< $(LDLIBS)

bin:
	mkdir -p bin

clean:
	rm -rf bin
