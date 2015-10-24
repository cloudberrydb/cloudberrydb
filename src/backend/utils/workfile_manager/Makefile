#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for utils/sfr
#
# IDENTIFICATION
#    $PostgreSQL: pgsql/src/backend/utils/sort/Makefile,v 1.15 2008/02/19 10:30:09 petere Exp $
#
#-------------------------------------------------------------------------

subdir = src/backend/utils/workfile_manager
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = workfile_mgr.o workfile_diskspace.o workfile_file.o workfile_mgr_test.o \
		workfile_segmentspace.o workfile_queryspace.o

include $(top_srcdir)/src/backend/common.mk
