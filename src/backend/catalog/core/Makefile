
subdir = src/backend/catalog/core
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = catcore.o catcoretable.o

include $(top_srcdir)/src/backend/common.mk

# FIXME: This is missing dependency on all the header files.
catcoretable.c: catcoregen.pl
	cat ../../../include/catalog/*.h | perl catcoregen.pl > catcoretable.c

clean:
	rm -f catcoretable.c
