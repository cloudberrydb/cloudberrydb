# contrib/dblink/Makefile

MODULE_big = dblink
OBJS = \
	$(WIN32RES) \
	dblink.o
PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = dblink
DATA = dblink--1.2.sql dblink--1.1--1.2.sql dblink--1.0--1.1.sql
PGFILEDESC = "dblink - connect to other PostgreSQL databases"

REGRESS = paths dblink
REGRESS_OPTS = --dlpath=$(top_builddir)/src/test/regress
EXTRA_CLEAN = sql/paths.sql expected/paths.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/dblink
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

REGRESS_OPTS += --init-file=$(top_builddir)/src/test/regress/init_file
