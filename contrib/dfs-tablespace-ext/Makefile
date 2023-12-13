MODULE_big = dfs_tablespace
OBJS = \
	$(WIN32RES) \
	dfs_option.o \
	dfs_tablespace.o \
	dfs_tablespace_hook.o \
	dfs_utils.o \
	ufs.o \
	ufs_connection.o \
	ufs_demo.o

EXTENSION = dfs_tablespace
DATA = dfs_tablespace--1.0.sql
PGFILEDESC = "dfs_tablespace - dfs tablespace"

HEADERS = ufs.h \
	ufs_connection.h \
	dfs_tablespace.h

REGRESS = dfs_tablespace

SHLIB_LINK += -lgopher -lvbf

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dfs_tablespace
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
