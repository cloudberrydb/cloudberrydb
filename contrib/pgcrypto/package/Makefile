all: gppkg

BLD_TOP=../../../gpAux
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

PGCRYPTO_DIR=`cd .. && pwd`
PGCRYPTO_VER=1.2
PGCRYPTO_PKG_VER=ossv1.1_pv1.2_gpdb4.3orca
PGCRYPTO_REL=2
PGCRYPTO_RPM_FLAGS="--define 'pgcrypto_dir $(PGCRYPTO_DIR)' --define 'pgcrypto_ver $(PGCRYPTO_VER)' --define 'pgcrypto_rel $(PGCRYPTO_REL)'"
PGCRYPTO_RPM=pgcrypto-$(PGCRYPTO_VER)-$(PGCRYPTO_REL).$(ARCH).rpm
PGCRYPTO_GPPKG=pgcrypto-$(PGCRYPTO_PKG_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(PGCRYPTO_GPPKG)
EXTRA_CLEAN+=$(PGCRYPTO_RPM) $(PGCRYPTO_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg: 
	$(MAKE) $(PGCRYPTO_RPM) RPM_FLAGS=$(PGCRYPTO_RPM_FLAGS)
	$(MAKE) $(PGCRYPTO_GPPKG) MAIN_RPM=$(PGCRYPTO_RPM)

.PHONY: gppkg
