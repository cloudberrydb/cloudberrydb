# $PostgreSQL: pgsql/contrib/varblocktest/Makefile,v 1.14 2005/09/27 17:13:08 tgl Exp $

PROGRAM = varblocktest
OBJS    = varblocktest.o cdbvarblock.o
top_builddir = ../..
include $(top_builddir)/src/Makefile.global


PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/varblocktest
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif




