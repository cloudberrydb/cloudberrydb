#------------------------------------------------------------------------
#
# Makefile for test crypto programs
#
# Copyright (c) 1998-2021, PostgreSQL Global Development Group
#
# src/test/crypto/Makefile
#
#------------------------------------------------------------------------

PGFILEDESC = "testcrypto - test PG crypto routines"
PGAPPICON=win32

subdir = src/test/crypto
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

export with_ssl

OBJS = \
	   $(WIN32RES) \
	   testcrypto.o

PROGS = testcrypto

all: $(PROGS)

testcrypto: $(OBJS) | submake-libpgport
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

check: temp-install $(PROGS)
	$(prove_check)

installcheck: $(PROGS)
	$(prove_installcheck)

clean distclean maintainer-clean:
	rm -f $(PROGS) $(OBJS)
	rm -rf tmp_check log
