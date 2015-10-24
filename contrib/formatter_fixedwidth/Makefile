MODULE_big = fixedwidth
OBJS       = fixedwidth.o

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = contrib/formatter_fixedwidth
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: install-data
install-data:
	$(INSTALL_DATA) fixedwidth.sql '$(DESTDIR)$(datadir)/cdb_init.d/fixedwidth.sql'

install: install-data

.PHONY: uninstall-data
uninstall-data:
	rm -f '$(DESTDIR)$(datadir)/cdb_init.d/fixedwidth.sql'

uninstall: uninstall-data
