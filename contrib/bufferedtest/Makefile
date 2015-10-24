# $PostgreSQL: pgsql/contrib/bufferedtest/Makefile,v 1.14 2005/09/27 17:13:08 tgl Exp $

PROGRAM = bufferedtest

top_builddir = ../..
include $(top_builddir)/src/Makefile.global


PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

OBJS=	bufferedtest.o cdbbufferedappend.o cdbbufferedread.o cdbvarblock.o lzjb.o

# Link against the Postgres backend.
SHLIB_LINK = $(BE_DLLLIBS)


# Shared library stuff
include $(top_srcdir)/src/Makefile.shlib
backend_src = $(top_srcdir)/src/backend

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/bufferedtest
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif




