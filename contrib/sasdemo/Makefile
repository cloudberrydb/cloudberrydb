
.PHONY: all clean install uninstall test

all:
	$(MAKE) -C formatter
	$(MAKE) -C receiver
	$(MAKE) -C sender
	$(MAKE) -C sas

test: install
	$(MAKE) -C test

clean:
	$(MAKE) -C formatter clean
	$(MAKE) -C sender clean
	$(MAKE) -C receiver clean
	$(MAKE) -C sas clean
	$(MAKE) -C test clean

install:
	$(MAKE) -C formatter install
	$(MAKE) -C sender install
	$(MAKE) -C receiver install
	$(MAKE) -C sas install

uninstall:
	$(MAKE) -C formatter uninstall
	$(MAKE) -C sender uninstall
	$(MAKE) -C receiver uninstall
	$(MAKE) -C sas uninstall