MODULE_big = gp_workfile_mgr
OBJS       = gp_workfile_cache_clear.o gp_workfile_cache_test.o gp_workfile_cache_stats.o

DATA_built = gp_workfile_mgr.sql
DATA = uninstall_gp_workfile_mgr.sql

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = src/bin/gp_workfile_mgr
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
