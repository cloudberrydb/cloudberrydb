all: gppkg

BLD_TOP=../../../../../../gpAux
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

DDBOOSTLIB_DIR=`cd .. && pwd`
DDBOOSTLIB_GPPKG_VER=2.6.2_pv1.2.1_gpdb4.3orca
DDBOOSTLIB_VER=2.6.2.0
DDBOOSTLIB_REL=2
DDBOOSTLIB_RPM_FLAGS="--define 'ddboostlib_dir $(DDBOOSTLIB_DIR)' --define 'ddboostlib_ver $(DDBOOSTLIB_VER)' --define 'ddboostlib_rel $(DDBOOSTLIB_REL)' --define 'bld_arch $(BLD_ARCH)'"
DDBOOSTLIB_RPM=ddboostlib-$(DDBOOSTLIB_VER)-$(DDBOOSTLIB_REL).$(ARCH).rpm
DDBOOSTLIB_GPPKG=ddboostlib-$(DDBOOSTLIB_GPPKG_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(DDBOOSTLIB_GPPKG)
EXTRA_CLEAN+=$(DDBOOSTLIB_RPM) $(DDBOOSTLIB_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg: 
	$(MAKE) $(DDBOOSTLIB_RPM) RPM_FLAGS=$(DDBOOSTLIB_RPM_FLAGS)
	$(MAKE) $(DDBOOSTLIB_GPPKG) MAIN_RPM=$(DDBOOSTLIB_RPM)

.PHONY: gppkg
