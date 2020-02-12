#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for gpopt
#
#-------------------------------------------------------------------------

subdir = src/backend/gpopt
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

include $(top_builddir)/src/backend/gporca/gporca.mk

SUBDIRS = config translate relcache utils

OBJS = CGPOptimizer.o gpdbwrappers.o

include $(top_srcdir)/src/backend/common.mk
