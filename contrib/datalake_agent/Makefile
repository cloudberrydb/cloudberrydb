top_builddir = ../..
include $(top_builddir)/src/Makefile.global

all:
	mvn package

clean:
	mvn clean

install: all
	$(MKDIR_P) $(pkglibdir)/java/
	$(INSTALL_DATA) 'target/dlagent-1.0.0.jar' $(pkglibdir)/java/

uninstall:
	rm -f $(pkglibdir)/java/dlagent-1.0.0.jar

distclean:
	mvn clean
