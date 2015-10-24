#-------------------------------------------------------------------------
#
# Makefile for src/tools/fsync
#
# Copyright (c) 2003-2009, PostgreSQL Global Development Group
#
# $PostgreSQL: pgsql/src/tools/fsync/Makefile,v 1.7 2009/01/01 17:24:05 momjian Exp $
#
#-------------------------------------------------------------------------

PROGRAM = test_fsync
OBJS    = test_fsync.o

subdir = src/tools/fsync
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)

all: submake-libpq submake-libpgport test_fsync
