# $PostgreSQL: pgsql/contrib/dblink/Makefile,v 1.9 2004/08/20 20:13:03 momjian Exp $

MODULE_big = xlogviewer
#PG_CPPFLAGS = -I$(libpq_srcdir)
OBJS	= xlogviewer.o
SHLIB_LINK = $(libpq)

DATA_built = xlogviewer.sql 
DOCS = README.xlogviewer
REGRESS = xlogviewer


ifdef USE_PGXS
PGXS = $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/xlogviewer
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
