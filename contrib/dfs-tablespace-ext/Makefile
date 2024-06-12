MODULE_big = dfs_tablespace
OBJS = \
	$(WIN32RES) \
	dfs_option.o \
	dfs_tablespace.o \
	dfs_tablespace_hook.o \
	dfs_utils.o \
	remotefile.o \
	remotefile_connection.o

EXTENSION = dfs_tablespace
DATA = dfs_tablespace--1.0.sql
PGFILEDESC = "dfs_tablespace - dfs tablespace"

SHLIB_LINK += -lgopher

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/dfs-tablespace-ext
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif