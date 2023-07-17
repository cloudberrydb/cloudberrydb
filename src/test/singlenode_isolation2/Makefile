#
# Makefile for isolation2 tests
#

subdir = src/test/isolation2
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

NAME = isolation2_regress
include $(top_srcdir)/src/Makefile.shlib

ifeq ($(PORTNAME), win32)
LDLIBS += -lws2_32
endif

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) -I$(srcdir)/../regress $(CPPFLAGS)
override LDLIBS := $(libpq_pgport) $(LDLIBS)

all: pg_isolation2_regress$(X) data

pg_regress.o:
	$(MAKE) -C $(top_builddir)/src/test/regress pg_regress.o
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/pg_regress.o .

gpstringsubs.pl:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/gpstringsubs.pl

gpdiff.pl: atmsort.pm explain.pm GPTest.pm
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/gpdiff.pl

GPTest.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/GPTest.pm

atmsort.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/atmsort.pm

explain.pm:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/explain.pm

data:
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/data

submake-isolation2:
	$(MAKE) -C $(top_builddir)/src/test/isolation2 install
	rm -rf isolation2_regress.so && $(LN_S) $(top_builddir)/src/test/isolation2/isolation2_regress.so

pg_isolation2_regress$(X): submake-isolation2 pg_regress.o submake-libpq submake-libpgport
	rm -rf pg_isolation2_regress && $(LN_S) $(top_builddir)/src/test/isolation2/pg_isolation2_regress

clean distclean:
	rm -f pg_regress.o
	rm -f isolation2_regress.so
	rm -f gpstringsubs.pl gpdiff.pl atmsort.pm explain.pm
	rm -f data
	rm -rf $(pg_regress_clean_files)

install: all gpdiff.pl gpstringsubs.pl

installcheck-singlenode: all
	(\
	gpconfig -c gp_appendonly_insert_files -v 0; \
	gpstop -u; \
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --schedule=$(srcdir)/isolation2_schedule \
	)
