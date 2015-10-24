SHELL=/bin/bash

default: all

BLD_TOP=$(shell echo `pwd`/../../)
include $(BLD_TOP)/Makefile.global
include $(BLD_TOP)/Makefile.thirdparty

EXT_DIR=$(BLD_TOP)/ext/$(BLD_ARCH)/

CC=$(strip $(BLD_CC))

SERVER_OBJS=gpnetbenchServer.o
CLIENT_OBJS=gpnetbenchClient.o

CFLAGS=-Werror -Wall -g -O2

ifeq (sol,$(findstring sol,$(BLD_ARCH)))
	GPNETBENCH_LIBS+=-lsocket -lnsl
endif

all: gpnetbenchServer gpnetbenchClient

gpnetbenchServer: $(SERVER_OBJS)
	$(CC) -o gpnetbenchServer $(SERVER_OBJS) $(GPNETBENCH_LIBS)

gpnetbenchClient: $(CLIENT_OBJS)
	$(CC) -o gpnetbenchClient $(CLIENT_OBJS) $(GPNETBENCH_LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -o $@ -c $<

clean:
	rm -rf *.o gpnetbenchServer gpnetbenchClient
