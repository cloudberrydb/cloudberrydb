#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for fts
#
#-------------------------------------------------------------------------

subdir = src/bin/gpfts
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

OBJS = fts_etcd.o fts.o ftsprobe.o etcd.o

FAILOVER_SCRIPTS = failover/master_check_back.sh failover/segment_all_down.sh failover/segment_check_back.sh failover/standby_check_back.sh \
	failover/docker_master_check_back.sh failover/docker_segment_check_back.sh failover/docker_standby_check_back.sh

ETCD_CONFIG_FILE = config/cbdb_etcd_default.conf

all: gpfts

gpfts: $(OBJS) | submake-libpgport $(SUBMAKE_LIBPQ)
	$(CC) $(CFLAGS) $(OBJS) $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

etcd.c: $(top_builddir)/src/backend/utils/etcd_lib/etcd.c
	ln -s $< $@

install: all installdirs
	$(INSTALL_PROGRAM) gpfts$(X) '$(DESTDIR)$(bindir)/gpfts$(X)'
	rm -rf $(DESTDIR)$(bindir)/failover
	mkdir $(DESTDIR)$(bindir)/failover
	for file in $(FAILOVER_SCRIPTS); do \
		$(INSTALL_SCRIPT) $$file '$(DESTDIR)$(bindir)/'$$file ; \
	done
	rm -rf $(DESTDIR)$(bindir)/config
	mkdir $(DESTDIR)$(bindir)/config
	$(INSTALL_SCRIPT) $(ETCD_CONFIG_FILE) $(DESTDIR)$(bindir)/$(ETCD_CONFIG_FILE)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/gpfts$(X)'

clean distclean maintainer-clean:
	rm -f gpfts$(X) $(OBJS)
	rm -f etcd.c
	rm -rf tmp_check
