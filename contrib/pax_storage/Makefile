# contrib/pax_storage/Makefile

MODULE_big = pax
OBJS = \
	$(WIN32RES) 
PG_CPPFLAGS = -I/usr/local/include
PG_CXXFLAGS = -std=c++14

PGFILEDESC = "pax - PAX table access method"
SHLIB_LINK += -luuid

PAX_REGRESS_DIR = src/test/regress
PAX_VEC_REGRESS_DIR = src/test/vec

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PGXS_DIR := $(dir $(PGXS))
PG_REGRESS = $(PGXS_DIR)/../../src/test/regress/pg_regress
else
subdir = contrib/pax_storage
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress
endif

.PHONY: all
all: build

DIRS = $(shell find src/cpp -maxdepth 2 -type d)
SRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.cc))

.PHONY: install-data
build: $(SRC)
	@echo "build pax"
	mkdir -p build
	cd build && cmake .. -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) && make -j8 && cp src/cpp/libpax.so ../pax.so

pax-unit-test: build
ifeq ($(enable_debug), yes)
	cd build && ./src/cpp/test_main
endif

install-data: build
	$(INSTALL_DATA) pax-cdbinit--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

.PHONY: install
install: install-data
	@echo "install data"
	@if [ -f build/src/cpp/pax_dump ]; then $(INSTALL_PROGRAM) build/src/cpp/pax_dump '$(DESTDIR)$(bindir)/pax_dump'; fi;
	make -C build install

.PHONY: pax-test
pax-test:
	$(PG_REGRESS) \
		--init-file=init_file --schedule=pax_schedule \
		--load-extension=gp_inject_fault --dbname=pax_test
	
.PHONY: icw_test
icw_test:
	@echo "Begin running pax regression tests"
	@echo "Current PGOPTIONS=$(PGOPTIONS)"
	make -C $(PAX_REGRESS_DIR) icw_test
	@echo "Begin running pax+vector regression tests"
	@echo "Current PGOPTIONS=$(PGOPTIONS) -c vector.enable_vectorization=on"
	PGOPTIONS="$(PGOPTIONS) -c vector.enable_vectorization=on" make -C $(PAX_VEC_REGRESS_DIR) icw_test
	@echo "Begin running pax+vec format+vector regression tests"
	@echo "Current PGOPTIONS=$(PGOPTIONS) -c vector.enable_vectorization=on -c pax_default_storage_format=porc_vec"
	PGOPTIONS="$(PGOPTIONS) -c vector.enable_vectorization=on -c pax_default_storage_format=porc_vec" make -C $(PAX_VEC_REGRESS_DIR) icw_test

.PHONY: uninstall-data
uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

uninstall: uninstall-data
clean-data:
	$(RM) pax-cdbinit--1.0.sql
	$(RM) -r build

clean: clean-data
