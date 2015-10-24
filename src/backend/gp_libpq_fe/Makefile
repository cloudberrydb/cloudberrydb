#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gp_libpq_fe subsystem (internal frontend half of libpq to talk from MasterSeg to SegDBs)
#
# IDENTIFICATION
#
#-------------------------------------------------------------------------

subdir = src/backend/gp_libpq_fe
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS :=  -DUNSAFE_STAT_OK -I$(srcdir) $(CPPFLAGS) -I$(top_builddir)/src/port

ifneq ($(PORTNAME), win32)
override CFLAGS += $(PTHREAD_CFLAGS)
endif

OBJS =  fe-exec.o pqexpbuffer.o \
    fe-auth.o   \
    fe-connect.o fe-misc.o fe-protocol3.o

ifeq ($(PORTNAME), win32)
OBJS += win32.o 
endif


include $(top_srcdir)/src/backend/common.mk