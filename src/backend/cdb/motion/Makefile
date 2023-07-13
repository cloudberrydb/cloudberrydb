#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for motion (CDB2)
#
#-------------------------------------------------------------------------


subdir = src/backend/cdb/motion
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

OBJS = cdbmotion.o tupchunklist.o tupser.o  \
	htupfifo.o tupleremap.o
ifeq ($(enable_ic_proxy),yes)
OBJS += ic_proxy_bgworker.o
endif  # enable_ic_proxy

include $(top_srcdir)/src/backend/common.mk
