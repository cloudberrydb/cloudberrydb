subdir = src/bin/gpfdist
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

ifeq ($(PORTNAME),win32)
  include $(top_builddir)/gpAux/Makefile.windows
endif

override CPPFLAGS := -I. $(CPPFLAGS)

OBJS = gpfdist.o gpfdist_helper.o fstream.o gfile.o

ifeq ($(have_yaml),yes)
  OBJS += transform.o
endif

ifneq ($(PORTNAME),win32)
  CPPFLAGS += -DGPFXDIST
else
  override CPPFLAGS := -I$(top_builddir)/src/port $(CPPFLAGS)
  OBJS += $(top_builddir)/src/port/glob.o
endif

LDLIBS += $(LIBS)

all: gpfdist$(EXE_EXT)

fstream.c: $(top_builddir)/src/backend/utils/misc/fstream/fstream.c
	ln -s $< $@

gfile.c: $(top_builddir)/src/backend/utils/misc/fstream/gfile.c
	ln -s $< $@

gpfdist$(EXE_EXT): $(OBJS)
	$(CC) $(LDFLAGS) $(OBJS) $(LDLIBS) -o $@

install: all
	$(INSTALL) gpfdist$(EXE_EXT) $(prefix)/bin

installcheck:
	$(MAKE) -C regress installcheck

.PHONY: clean distclean
clean:
	rm -f $(OBJS) gpfdist$(EXE_EXT)

distclean: clean
	rm -f fstream.c gfile.c GNUmakefile config.log config.status
	rm -rf autom4te.cache
	$(MAKE) -C regress clean
