#-------------------------------------------------------------------------
#
# Makefile for src/bin/gp_mount_agent
#
# Portions Copyright (c) 2010 Greenplum Inc 
#
# This Makefile was copied from the gpmirrortransition makefile and modified accordingly
#
# $Version$
#
#-------------------------------------------------------------------------

PGFILEDESC = "gp_mount_agent - utility "
subdir = src/bin/gp_mount_agent
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

# The frontend doesn't need everything that's in LIBS, some are backend only
LIBS := $(filter-out -lresolv, $(LIBS))
# This program isn't interactive, so doesn't need these
LIBS := $(filter-out -lreadline -ledit -ltermcap -lncurses -lcurses -lcurl -lssl -lcrypto, $(LIBS))

# the use of tempnam in pg_backup_tar.c causes a warning when using newer versions of GCC
#override CPPFLAGS := -Wno-deprecated-declarations -DFRONTEND -I$(libpq_srcdir) $(CPPFLAGS)
override CPPFLAGS :=

override LDFLAGS := -static

OBJS=gp_mount_agent.o $(WIN32RES)

EXTRA_OBJS = $(top_builddir)/src/backend/libpq/ip.o 

all: gp_mount_agent

gp_primarymirror: gp_mount_agent.o $(OBJS) $(libpq_builddir)/libpq.a 
	$(CC) $(CFLAGS) $(OBJS) $(EXTRA_OBJS) $(libpq_pgport) $(LDFLAGS) $(LIBS) -o $@$(X)

.PHONY: submake-backend
submake-backend:
	$(MAKE)

install: all installdirs
	$(INSTALL_PROGRAM) gp_mount_agent$(X) '$(DESTDIR)$(bindir)'/gp_mount_agent$(X)

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f $(addprefix '$(DESTDIR)$(bindir)'/, gp_mount_agent$(X))

clean distclean maintainer-clean:
	rm -f gp_mount_agent$(X) $(OBJS) gp_mount_agent.o
