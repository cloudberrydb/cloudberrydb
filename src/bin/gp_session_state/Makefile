MODULE_big = gp_session_state
OBJS       = gp_session_state_memory_stats.o

DATA_built = gp_session_state.sql
DATA = uninstall_gp_session_state.sql

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = src/bin/gp_session_state
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
