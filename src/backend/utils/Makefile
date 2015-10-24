#
# Makefile for utils
#
# $PostgreSQL: pgsql/src/backend/utils/Makefile,v 1.24 2006/07/15 03:33:14 tgl Exp $
#

subdir = src/backend/utils
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS        = fmgrtab.o session_state.o
SUBDIRS     = adt cache datumstream error fmgr hash init mb misc mmgr resowner \
			   resscheduler sort stat time gpmon gp workfile_manager

include $(top_srcdir)/src/backend/common.mk

all: fmgroids.h probes.h


$(SUBDIRS:%=%-recursive): fmgroids.h


fmgroids.h fmgrtab.c: Gen_fmgrtab.sh $(top_srcdir)/src/include/catalog/pg_proc.h
	AWK='$(AWK)' $(SHELL) $< $(top_srcdir)/src/include/catalog/pg_proc.h

ifneq ($(enable_dtrace), yes)
probes.h: Gen_dummy_probes.sed
endif

probes.h: probes.d
ifeq ($(enable_dtrace), xxxyes)
	$(DTRACE) -C -s $< -o $@.tmp
	sed -e 's/POSTGRESQL_/TRACE_POSTGRESQL_/g' $@.tmp >$@
	rm $@.tmp
else
	sed -f $(srcdir)/Gen_dummy_probes.sed $< >$@
endif


clean:
	for dir in $(SUBDIRS); do $(MAKE) -C $$dir $@ || exit; done
	rm -f SUBSYS.o fmgrtab.o fmgroids.h fmgrtab.c probes.h
