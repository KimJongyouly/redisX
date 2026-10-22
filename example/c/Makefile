CC ?= cc
PKG_CONFIG ?= pkg-config
HIREDIS_PREFIX := $(shell brew --prefix hiredis 2>/dev/null)

ifeq ($(shell $(PKG_CONFIG) --exists hiredis 2>/dev/null; echo $$?),0)
  CFLAGS += $(shell $(PKG_CONFIG) --cflags hiredis)
  LDLIBS += $(shell $(PKG_CONFIG) --libs hiredis)
else
  CFLAGS += -I$(HIREDIS_PREFIX)/include
  LDLIBS += -L$(HIREDIS_PREFIX)/lib -lhiredis
endif

CFLAGS += -Wall -Wextra -std=c11
LDLIBS += -lm

SRCS := string.c list.c hash.c set.c zset.c hyperloglog.c slowlog.c dataframe.c pubsub.c
BINS := $(SRCS:%.c=bin/%)

.PHONY: all clean
all: $(BINS)

bin/%: %.c common.h | bin
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

bin:
	mkdir -p bin

clean:
	rm -rf bin
