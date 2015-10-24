.PHONY: gpsmon gpmmon

all: gpsmon gpmmon

gpsmon:
	$(MAKE) -C src/gpmon gpsmon BUILDDIR=$(BUILDDIR)

gpmmon:
	$(MAKE) -C src/gpmon gpmmon BUILDDIR=$(BUILDDIR)

webapi:
	@echo webapi

clean:
	$(MAKE) -C src/gpmon clean
