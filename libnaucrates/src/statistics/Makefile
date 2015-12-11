
BLD_TOP := ../../..
include $(BLD_TOP)/make/gpo.mk

INCDIRS = -I $(BLD_TOP)/libnaucrates/include -I $(LIBGPOS)/include -I $(XERCES)/include -I $(BLD_TOP)/libgpopt/include 

# add 'no long long'
CFLAGS_OPTIONAL = -Wconversion -Wno-long-long

all: objs
