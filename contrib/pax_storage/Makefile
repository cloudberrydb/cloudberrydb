# contrib/pax_storage/Makefile

MODULE_big = pax
OBJS = \
	$(WIN32RES) 
PG_CPPFLAGS = -I/usr/local/include
PG_CXXFLAGS = -std=c++14

PGFILEDESC = "pax - PAX table access method"
SHLIB_LINK += -luuid

REGRESS = setup
REGRESS += alter_distributed detoast ddl numeric types update filter
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

REGRESS_OPTS += --init-file=init_file

.PHONY: all
all: build

DIRS = $(shell find src/cpp -maxdepth 2 -type d)
SRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.cc))

.PHONY: install-data
build: $(SRC)
	@echo "build pax"
	mkdir -p build
	cd build && cmake .. -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) && make -j8 && cp src/cpp/libpax.so ../pax.so

pax-test: build
	cd build && ./src/cpp/test_main

install-data: build
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
