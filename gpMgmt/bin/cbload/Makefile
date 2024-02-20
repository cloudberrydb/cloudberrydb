subdir = gpMgmt/bin/cbload
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

.DEFAULT_GOAL := all

export GOPROXY ?= https://proxy.golang.org

all: build

build :
	go mod download
	go build -o cbload github.com/cloudberrydb/cbload

clean :
	rm -f cbload

install: all
	$(INSTALL_PROGRAM) 'cbload' $(bindir)

uninstall:
	rm -f $(bindir)/cbload

distclean:
	rm -f cbload
