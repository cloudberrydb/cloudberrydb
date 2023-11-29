# gpcontrib/datalake_proxy/Makefile

MODULE_big = datalake_proxy
OBJS = \
	$(WIN32RES) \
	datalake_proxy.o

EXTENSION = datalake_proxy

PGFILEDESC = "datalake_proxy - Datalake Proxy"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = gpcontrib/datalake_proxy
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif