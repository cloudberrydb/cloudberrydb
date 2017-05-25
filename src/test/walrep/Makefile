MODULES=gplibpq
PG_CONFIG=pg_config

subdir = src/test/walrep/
top_builddir = ../../..

include $(top_builddir)/src/Makefile.global

REGRESS = setup walreceiver
ifeq ($(enable_segwalrep), yes)
REGRESS += generate_ao_xlog generate_aoco_xlog
endif
REGRESS_OPTS = --dbname="walrep_regression"

NO_PGXS = 1
include $(top_srcdir)/src/makefiles/pgxs.mk
