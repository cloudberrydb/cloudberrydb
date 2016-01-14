##-------------------------------------------------------------------------------------
##
## Copyright (C) 2008 - 2010 Greenplum Inc.
##
## @doc: top-level Makefile
##
##
##-------------------------------------------------------------------------------------

BLD_TOP := $(shell sh -c pwd)

LIB_VERSION = 1.134

include make/gpo.mk

## ----------------------------------------------------------------------
## top level variables; only used in this makefile
## ----------------------------------------------------------------------

# additional MAKE flags for automated builds
AUTO_MAKE_FLAGS = --no-print-directory -s

# default build arch; overridden by PULSE
BLD_ARCH = matrix

# components to build; each has their own Makefile with default target 'all'
SUBDIRS = libgpos server

# test components; directories in which to run tests
TESTDIRS = server

# test parameter
TEST_PARAM = -u

## ======================================================================
## TARGETS
## ======================================================================

## ----------------------------------------------------------------------
## developer targets
## ----------------------------------------------------------------------

all:
	@$(MAKE) subtargets
	@mkdir -p $(OBJDIR_GENERAL)
	@PATH=$(PMCCABE):$$PATH ./make/complexity.sh . $(OBJDIR_GENERAL)

build_all_types:
	@for t in $(BLD_TYPE_ALL); do $(MAKE) $$t || exit; done

test:
	@for d in $(TESTDIRS); do $(MAKE) --directory=$$d test BLD_TOP=$(BLD_TOP) TEST_PARAM=$(TEST_PARAM); done

test_ext:
	@$(MAKE) test TEST_PARAM=-x

test_all_types:
	@for t in $(BLD_TYPE_ALL); do $(MAKE) test BLD_TYPE=$$t || exit; done

# build all types for both 32/64 bits then run tests
matrix:
	@for b in $(ARCH_BIT_ALL); do $(MAKE) BLD_TOP=$(BLD_TOP) ARCH_BIT=$$b build_all_types || exit; done
	@for b in $(ARCH_BIT_ALL); do $(MAKE) BLD_TOP=$(BLD_TOP) ARCH_BIT=$$b test_all_types ; done

LIBGPOS = /opt/releng/tools/emc/libgpos/$(LIB_VERSION)/$(shell GPOS_BIT=$(ARCH_BIT) $(BLD_TOP)/make/platform_defines.sh)/libgpos

copy_libs:
	echo "Copying libgpos to $(LIBGPOS)/$(OBJDIR_DEFAULT)"
	sudo cp $(BLD_TOP)/libgpos/$(OBJDIR_DEFAULT)/libgpos.$(LDSFX) $(LIBGPOS)/$(OBJDIR_DEFAULT)

copy_headers:
	echo "Copying headers to $(LIBGPOS)/include"
	sudo rm -rf $(LIBGPOS)/libgpos/include 
	sudo cp -r libgpos/include $(LIBGPOS) 

copy_all: copy_libs copy_headers

## ----------------------------------------------------------------------
## automation targets
## ----------------------------------------------------------------------

# target for automated builds
auto:
	@$(MAKE) $(AUTO_MAKE_FLAGS) $(BLD_ARCH)

auto_ext:
	@$(MAKE) auto TEST_PARAM=-x

# anything ending in _64 is a 64bit build...
%_64: 
	$(MAKE) ARCH_BIT=GPOS_64BIT build_all_types test_all_types

# ...and _32 denotes 32bit
%_32:
	$(MAKE) ARCH_BIT=GPOS_32BIT build_all_types test_all_types

# Mac OS 10.x is the only architecture for which we can 
# build both 32 and 64 on one box
osx10%_x86:
	$(MAKE) $(AUTO_MAKE_FLAGS) matrix

## ----------------------------------------------------------------------
## special targets
## ----------------------------------------------------------------------

# tar up files excluding all build artifacts
ARCHIVE = gpo.$(shell date "+20%y-%m-%d.%H.%M.%S").$(USER).tgz
tar:
	$(TAR) cfz $(ARCHIVE) --exclude \.obj\* --exclude \*tgz .

## ----------------------------------------------------------------------
## Sync/Clean tools
## ----------------------------------------------------------------------
## Populate/clean up dependent releng supported tools.  The projects are
## downloaded and installed into /opt/releng/tools/...
##
## Tool dependencies and platform config mappings are defined in two
## locations.  Using Xerces as an example, the following files will
## need to be udpated.  See top-level README file for additional info.
##
##   * Makefile XERCES location variable definition:
##      optimizer/make/gpo.mk
##   * Apache Ivy dependency definition file
##       optimizer/make/dependencies/ivy.xml
## ----------------------------------------------------------------------

/opt/releng/apache-ant:
	@echo Syncing supporting external tools ...
	@curl --silent http://releng.sanmateo.greenplum.com/tools/apache-ant.1.8.1.tar.gz -o /tmp/apache-ant.1.8.1.tar.gz
	@( umask 002; [ ! -d /opt/releng ] && mkdir -p /opt/releng; \
	   cd /opt/releng; \
	   gunzip -qc /tmp/apache-ant.1.8.1.tar.gz | tar xf -; \
	   rm -f /tmp/apache-ant.1.8.1.tar.gz; \
	   chmod -R a+w /opt/releng/apache-ant)

sync_tools: /opt/releng/apache-ant
	@LCK_FILES=$$( find /opt -name *.lck ); \
	if [ -n "$${LCK_FILES}" ]; then \
		echo "Removing existing .lck files!"; \
		find /opt -name *.lck | xargs rm; \
	fi

	@if [ ! -w /opt ]; then \
	    echo ""; \
	    echo "======================================================================"; \
	    echo "ERROR: /opt is not writable."; \
	    echo "----------------------------------------------------------------------"; \
	    echo "  The Ivy third-party cache is stored in /opt.  Please ensure you have"; \
	    echo "  write access to /opt"; \
	    echo "======================================================================"; \
	    echo ""; \
	    exit 1; \
	fi	
	@for b in $(ARCH_BIT_ALL); do \
		echo $(BLD_TOP); \
		BLD_ARCH=$$( GPOS_BIT=$$b $(BLD_TOP)/make/platform_defines.sh ); \
		cd make/dependencies; \
		(umask 002; ANT_OPTS="-Djavax.net.ssl.trustStore=$(BLD_TOP)/make/dependencies/cacerts" /opt/releng/apache-ant/bin/ant -DBLD_ARCH=$${BLD_ARCH} resolve); \
		cd ../..; \
	done

clean_tools:
	@cd make/dependencies; \
	/opt/releng/apache-ant/bin/ant clean; \
	rm -rf /opt/releng/apache-ant; \

check_coverity_env:
ifndef COVERITY_STREAM
	$(error COVERITY_STREAM is undefined)
endif
ifndef COVERITY_PROJECT
	$(error COVERITY_PROJECT is undefined)
endif

sync_coverity:
	@if [ ! -w /opt ]; then \
		echo ""; \
		echo "======================================================================"; \
	    echo "ERROR: /opt is not writable."; \
	    echo "----------------------------------------------------------------------"; \
	    echo "  The Ivy third-party cache is stored in /opt.  Please ensure you have"; \
	    echo "  write access to /opt"; \
	    echo "======================================================================"; \
	    echo ""; \
	    exit 1; \
	fi	
	@for b in $(ARCH_BIT_ALL); do \
		echo $(BLD_TOP); \
		BLD_ARCH=$$( GPOS_BIT=$$b $(BLD_TOP)/make/platform_defines.sh ); \
		cd make/dependencies; \
		(umask 002; ANT_OPTS="-Djavax.net.ssl.trustStore=$(BLD_TOP)/make/dependencies/cacerts" /opt/releng/apache-ant/bin/ant -DBLD_ARCH=$${BLD_ARCH} resolve); \
		cd ../..; \
	done

assign_coverity_defect_owner: check_coverity_env
	@if [ ! -L "/opt/ActivePerl-5.18" ]; then \
		ln -s /opt/releng/tools/perl/perl/5.18/rhel5_x86_64/perl /opt/ActivePerl-5.18 ;\
	fi
	@export PATH=/opt/ActivePerl-5.18/bin:${PATH} ;\
	which perl ;\
	cd ${BLD_TOP}/.. ;\
	perl ${BLD_TOP}/assign-owner-to-unassigned-cids.pl \
		--project "${COVERITY_PROJECT}" \
		--config ${BLD_TOP}/Coverity/coverity_pse_config.xml \
		--line-number ${DRY_RUN}
	unlink /opt/ActivePerl-5.18

check_coverity_defects: check_coverity_env
	@export PYTHONPATH=${BLD_TOP}/../ext/coverity_${BLD_ARCH}/suds:${PYTHONPATH} ;\
	python ${BLD_TOP}/Coverity/findDefects.py \
		--host "coverity.greenplum.com" \
		--port "8080" \
		--user admin \
		--password 9c3962328041ddc65fab634e87f21de5 \
		--project "${COVERITY_PROJECT}" \
		--stream "${COVERITY_STREAM}" \
		--compareSnapshotId ""


## ----------------------------------------------------------------------
## eof
## ----------------------------------------------------------------------
