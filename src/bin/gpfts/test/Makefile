subdir=src/bin/gpfts
top_builddir=../../../..
etcdlib_obj=$(top_builddir)/src/backend/utils/etcd_lib/etcd.o
include $(top_builddir)/src/Makefile.global


TARGETS=gpfts

override CPPFLAGS+= -I$(top_srcdir)/src/interfaces/libpq
override CPPFLAGS+= -I$(top_srcdir)/src/bin/gpfts

include $(top_srcdir)/src/Makefile.mock
LDFLAGS := -luuid $(LDFLAGS)

gpfts.t: gp_fts_test.o $(CMOCKERY_OBJS) ../fts_etcd.o $(etcdlib_obj) $(top_srcdir)/src/fe_utils/log.o
	$(CC) $^ $(libpq_pgport) $(LDFLAGS) $(rpath) $(LIBS) $(libpq) -o $@
