# contrib/pax_storage/Makefile

MODULE_big = vbf
OBJS = \
	$(WIN32RES) 

PGFILEDESC = "vbf - append-only file"

subdir = contrib/vbf
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

.PHONY: all install-vbf install uninstall-data uninstall clean-data clean
all: build

DIRS = $(shell find src -maxdepth 2 -type d)
SRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.c))
SRC += examples/quickstart.c

build: ${SRC}
	@./version.sh project
	mkdir -p build  && cd build && cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=${prefix} -DCMAKE_INSTALL_LIBDIR=lib && make && cp src/libvbf.so ../vbf.so

install-vbf: build
	make -C build install

install: install-vbf

uninstall-data:
	$(RM) vbf.so

uninstall: uninstall-data

clean-data:
	$(RM) -r build

clean: clean-data