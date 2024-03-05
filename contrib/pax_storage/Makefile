# contrib/pax_storage/Makefile

MODULE_big = pax
OBJS = \
	$(WIN32RES) 
PG_CPPFLAGS = -I/usr/local/include
PG_CXXFLAGS = -std=c++14

PGFILEDESC = "pax - PAX table access method"
SHLIB_LINK += -luuid

REGRESS = setup
REGRESS += detoast ddl types update
# FIXME: several plans are bad in update_gp when use orca
# REGRESS += update_gp
REGRESS +=  teardown

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/pax_storage
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

REGRESS_OPTS += --init-file=$(top_builddir)/src/test/regress/init_file

.PHONY: all
all: build-pax

.PHONY: install-data build-pax
build-pax:
	@echo "build pax"
	mkdir -p build
	cd build && cmake .. -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) && make -j8 && cp src/cpp/libpax.so ../pax.so

pax-test: build-pax
	cd build && ./src/cpp/test_main

install-data: build-pax
	$(INSTALL_DATA) pax-cdbinit--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

installcheck: pax-test

.PHONY: install
install: install-data
	@echo "install data"
	make -C build install

.PHONY: uninstall-data

uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

uninstall: uninstall-data

clean-data:
	$(RM) pax-cdbinit--1.0.sql
	$(RM) -r build

clean: clean-data
