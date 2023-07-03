#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for src/backend/task
#
# IDENTIFICATION
#    src/backend/task/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/task
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
override CPPFLAGS := -I$(libpq_srcdir) -Wno-implicit-fallthrough $(CPPFLAGS)

OBJS = \
	entry.o \
	job_metadata.o \
	misc.o \
	pg_cron.o \
	task_states.o

include $(top_srcdir)/src/backend/common.mk
