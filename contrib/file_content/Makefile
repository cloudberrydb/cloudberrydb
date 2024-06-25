# contrib/file_content/Makefile

MODULE_big = file_content

OBJS = \
	$(WIN32RES) \
	file_content.o

EXTENSION = file_content
DATA = file_content--1.0.sql
PGFILEDESC = "file_content - file_content function for directory table"

REGRESS = file_content

EXTRA_CLEAN = sql/file_content.sql expected/file_content.out

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/file_content
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
