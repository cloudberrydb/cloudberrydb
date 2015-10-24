MODULE_big = gp_distribution_policy
OBJS       = gp_distribution_policy.o
DATA_built = gp_distribution_policy.sql
DATA = uninstall_gp_distribution_policy.sql
PG_CPPFLAGS = -I$(libpq_srcdir)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/gp_distribution_policy
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

