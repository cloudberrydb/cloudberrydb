#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_builddir)/src/backend/gporca/libgpos/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_builddir)/src/backend/gporca/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_builddir)/src/backend/gporca/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_builddir)/src/backend/gporca/libgpdbcost/include $(CPPFLAGS)


SUBDIRS = config translate relcache utils

OBJS = CGPOptimizer.o gpdbwrappers.o

include $(top_srcdir)/src/backend/common.mk
