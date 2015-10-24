#
# Makefile for storage/ipc
#
# $PostgreSQL: pgsql/src/backend/storage/ipc/Makefile,v 1.19 2005/05/19 21:35:46 tgl Exp $
#

subdir = src/backend/storage/ipc
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(top_srcdir)/src/backend/gp_libpq_fe $(CPPFLAGS)

# seems to be required 1999/07/22 bjm
ifeq "$(findstring alpha,$(host_cpu))" "alpha"
ifeq "$(GCC)" "yes"
override CFLAGS+= -fno-inline
endif
endif

OBJS = ipc.o ipci.o pmsignal.o procarray.o procsignal.o shmem.o shmqueue.o \
	sinval.o sinvaladt.o

include $(top_srcdir)/src/backend/common.mk
