MODULES=fsync_helper
PG_CONFIG=pg_config

REGRESS = setup bgwriter_checkpoint
REGRESS_OPTS = --dbname="fsync_regression"

subdir = src/test/fsync/
top_builddir = ../../..

include $(top_builddir)/src/Makefile.global

NO_PGXS = 1
include $(top_srcdir)/src/makefiles/pgxs.mk
