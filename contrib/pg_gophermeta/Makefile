# contrib/pg_gophermeta/Makefile
MODULE_big = pg_gophermeta
OBJS = \
	$(WIN32RES) \
	pg_gophermeta.o

EXTENSION = pg_gophermeta

SHLIB_LINK += -lgopher

PGFILEDESC = "pg_gophermeta - File Cache System"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pg_gophermeta
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
