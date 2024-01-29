# contrib/pax_storage/Makefile

MODULE_big = vbf
OBJS = \
	$(WIN32RES) 

PGFILEDESC = "vbf - append-only file"

subdir = contrib/vbf
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk

.PHONY: all build-vbf install-vbf install uninstall-data uninstall clean-data clean
all: build-vbf

build-vbf:
	@./version.sh project
	mkdir -p build  && cd build && cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_INSTALL_PREFIX=${prefix} -DCMAKE_INSTALL_LIBDIR=lib && make && cp src/libvbf.so ../vbf.so

install-vbf: build-vbf
	make -C build install

install: install-vbf

uninstall-data:
	$(RM) vbf.so

uninstall: uninstall-data

clean-data:
	$(RM) -r build

clean: clean-data