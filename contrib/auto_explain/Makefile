# contrib/auto_explain/Makefile

MODULE_big = auto_explain
OBJS = \
	$(WIN32RES) \
	auto_explain.o
PGFILEDESC = "auto_explain - logging facility for execution plans"

REGRESS = auto_explain
REGRESS_OPTS = --init-file=init_file
TAP_TESTS = 1

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/auto_explain
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
