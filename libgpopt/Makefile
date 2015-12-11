
BLD_TOP := ..
include $(BLD_TOP)/make/gpo.mk

SUBDIRS = src

LIB_NAME = libgpopt
LIB_VERSION = 1.93
LIB_COMPAT_VERSION = 1.0

LDLIBS := $(LDLIBS) -lgpos -lnaucrates -lxerces-c
LDFLAGS = -L$(LIBGPOS_LIBDIR) -L$(BLD_TOP)/libnaucrates/$(OBJDIR) -L$(XERCES_LIBDIR)

all: lib
