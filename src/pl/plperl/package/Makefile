all: gppkg

BLD_TOP=../../../../gpAux
GPMGMT=$(BLD_TOP)/gpMgmt
include $(BLD_TOP)/releng/tools.mk
include $(BLD_TOP)/releng/releng.mk
include $(BLD_TOP)/Makefile.global

OS=$(word 1,$(subst _, ,$(BLD_ARCH)))
ARCH=$(shell uname -p)

# NOTE: PERL_DIR is dfined in $(BLD_TOP)/releng/releng.mk

PLPERL_GPPKG_VER=ossv5.12.4_pv1.3_gpdb4.3orca
PLPERL_VER=1.3
PLPERL_REL=4
PLPERL_DIR=`cd .. && pwd`
PLPERL_RPM_FLAGS="--define 'plperl_dir $(PLPERL_DIR)' --define 'plperl_ver $(PLPERL_VER)' --define 'plperl_rel $(PLPERL_REL)' "
PLPERL_RPM=plperl-$(PLPERL_VER)-$(PLPERL_REL).$(ARCH).rpm
PLPERL_GPPKG=plperl-$(PLPERL_GPPKG_VER)-$(OS)-$(ARCH).gppkg

TARGET_GPPKG=$(PLPERL_GPPKG)
EXTRA_CLEAN+=$(PLPERL_RPM) $(PLPERL_GPPKG)

#
# Generic rules to build gppkgs included here
#
include $(BLD_TOP)/releng/gppkg.mk

gppkg:
	$(MAKE) $(PLPERL_RPM) RPM_FLAGS=$(PLPERL_RPM_FLAGS)
	$(MAKE) $(PLPERL_GPPKG) MAIN_RPM=$(PLPERL_RPM) 

.PHONY: gppkg
