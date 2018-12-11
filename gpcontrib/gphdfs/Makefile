MODULE_big = gphdfs
OBJS = gphdfsformatter.o gphdfsprotocol.o
REGRESS = basic privileges

PG_CPPFLAGS = -I$(libpq_srcdir)
PG_LIBS = $(libpq_pgport)

ANT = ant
TAR = tar
ANT_OPTS="-Djavax.net.ssl.trustStore=../../gpAux/releng/make/dependencies/cacerts"

ifdef USE_PGXS
	PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
	top_builddir = ../..
	include $(top_builddir)/src/Makefile.global
	include $(top_builddir)/contrib/contrib-global.mk
endif

JAR_FILES = dist/hadoop-gnet-1.2.0.0.jar

all: $(JAR_FILES) unittest

dist/hadoop-gnet-1.2.0.0.jar:
	$(ANT) clean
	ANT_OPTS=$(ANT_OPTS) $(ANT) dist -Dgphdgnet.version=hadoop-gnet-1.2.0.0 \
                -Dgpgnet.src=1.2 -Dgpgnet.configuration=hadoop2
	cp dist/hadoop-gnet-1.2.0.0.jar dist/hdp-gnet-1.2.0.0.jar
	cp dist/hadoop-gnet-1.2.0.0.jar dist/cdh-gnet-1.2.0.0.jar
	cp dist/hadoop-gnet-1.2.0.0.jar dist/mpr-gnet-1.2.0.0.jar

unittest: $(JAR_FILES)
	$(ANT) clean
	ANT_OPTS=$(ANT_OPTS) $(ANT) test -Dgphdgnet.version=hadoop-gnet-1.2.0.0 \
                -Dgpgnet.src=1.2 -Dgpgnet.configuration=ut

install-jars: $(JAR_FILES)
	$(INSTALL_PROGRAM) dist/*.jar '$(DESTDIR)$(libdir)/hadoop/'
	$(INSTALL_PROGRAM) hadoop_env.sh$(X) '$(DESTDIR)$(libdir)/hadoop/hadoop_env.sh$(X)'
	$(INSTALL_PROGRAM) jaas.conf$(X) '$(DESTDIR)$(libdir)/hadoop/jaas.conf$(X)'

.PHONY: install-gphdfs
install-gphdfs: gphdfs.so
	$(INSTALL_PROGRAM) gphdfs.so $(DESTDIR)$(pkglibdir)
	$(INSTALL_PROGRAM) gphdfs.sql '$(DESTDIR)$(datadir)/cdb_init.d/gphdfs.sql'

install: install-jars install-gphdfs

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(libdir)/hadoop'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/*.jar'
	rm -f '$(DESTDIR)$(bindir)/hadoop_env.sh$(X)'
	rm -f '$(DESTDIR)$(bindir)/jaas.conf$(X)'
	rm -f '$(DESTDIR)$(datadir)/cdb_init.d/gphdfs.sql'

clean: clean-extras

clean-extras:
	$(ANT) clean
	rm -rf *-javadoc
	rm -f *-javadoc.tar
	rm -rf result
