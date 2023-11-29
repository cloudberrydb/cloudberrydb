.PHONY: all clean install uninstall distclean

all:
	$(MAKE) -C native
	$(MAKE) -C java

clean:
	$(MAKE) -C native clean
	$(MAKE) -C java clean

install:
	$(MAKE) -C native install
	$(MAKE) -C java install

uninstall:
	$(MAKE) -C native uninstall
	$(MAKE) -C java uninstall

distclean:
	$(MAKE) -C native distclean
	$(MAKE) -C java distclean
