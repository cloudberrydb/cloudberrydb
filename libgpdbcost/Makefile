
BLD_TOP := ..
include $(BLD_TOP)/make/gpo.mk

SUBDIRS = src

LIB_NAME = libgpdbcost
LIB_VERSION = 1.0
LIB_COMPAT_VERSION = 1.0

LDLIBS := $(LDLIBS) -lgpos 
LDFLAGS = -L$(LIBGPOS_LIBDIR)

all: lib
