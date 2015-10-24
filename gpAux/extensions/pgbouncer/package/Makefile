all: gppkg

BLD_TOP=../../../
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

PGBOUNCER_DIR=`cd ../source && pwd`
PGBOUNCER_VER=1.2
PGBOUNCER_PKG_VER=ossv1.1_pv1.2_gpdb4.3
PGBOUNCER_REL=1
PGBOUNCER_RPM_FLAGS="--define 'pgbouncer_dir $(PGBOUNCER_DIR)' --define 'pgbouncer_ver $(PGBOUNCER_VER)' --define 'pgbouncer_rel $(PGBOUNCER_REL)'"
PGBOUNCER_RPM=pgbouncer-$(PGBOUNCER_VER)-$(PGBOUNCER_REL).$(ARCH).rpm
PGBOUNCER_GPPKG=pgbouncer-$(PGBOUNCER_PKG_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(PGBOUNCER_GPPKG)
EXTRA_CLEAN+=$(PGBOUNCER_RPM) $(PGBOUNCER_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg: 
	$(MAKE) $(PGBOUNCER_RPM) RPM_FLAGS=$(PGBOUNCER_RPM_FLAGS)
	$(MAKE) $(PGBOUNCER_GPPKG) MAIN_RPM=$(PGBOUNCER_RPM)

.PHONY: gppkg
