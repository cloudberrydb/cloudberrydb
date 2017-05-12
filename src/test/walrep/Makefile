MODULES=gplibpq
PG_CONFIG=pg_config

REGRESS = setup walreceiver
REGRESS_OPTS = --dbname="walrep_regression"

subdir = src/test/walrep/
top_builddir = ../../..

include $(top_builddir)/src/Makefile.global

NO_PGXS = 1
include $(top_srcdir)/src/makefiles/pgxs.mk
