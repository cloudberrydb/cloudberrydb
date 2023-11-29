top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

all:
	mvn package

clean:
	mvn clean

install: all
	$(MKDIR_P) $(pkglibdir)/java/
	$(INSTALL_DATA) 'target/libhms-1.0.0-jar-with-dependencies.jar' $(pkglibdir)/java/

uninstall:
	rm -f $(pkglibdir)/java/libhms-1.0.0-jar-with-dependencies.jar

distclean:
	mvn clean
