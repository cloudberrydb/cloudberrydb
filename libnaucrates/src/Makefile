
BLD_TOP := ../..
include $(BLD_TOP)/make/gpo.mk

INCDIRS= -I $(BLD_TOP)/libnaucrates/include -I $(LIBGPOS)/include -I $(BLD_TOP)/libgpopt/include -I $(BLD_TOP)/libgpdbcost/include -I $(XERCES)/include 
SUBDIRS = base comm md operators parser statistics xml

# add 'no long long'
CFLAGS_OPTIONAL = -Wno-long-long

all: objs


