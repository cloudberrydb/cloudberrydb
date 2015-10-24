MODULE_big = gp_cancel_query
OBJS       = gp_cancel_query.o

PG_CPPFLAGS = -I$(libpq_srcdir)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/gp_cancel_query
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
