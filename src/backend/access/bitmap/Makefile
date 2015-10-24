#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for access/bitmap
#
#  Copyright (c) 2006, PostgreSQL Global Development Group
#
# IDENTIFICATION
#    $PostgreSQL$
#
#-------------------------------------------------------------------------

subdir = src/backend/access/bitmap
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = bitmaputil.o bitmapattutil.o \
	bitmappages.o bitmapinsert.o bitmapsearch.o bitmap.o bitmapxlog.o

include $(top_srcdir)/src/backend/common.mk

