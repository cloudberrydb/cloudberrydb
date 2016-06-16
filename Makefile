#-------------------------------------------------------------------------------------
#
# Copyright (C) 2008 - 2011 EMC Corp.
#
# @doc: top-level Makefile
#
#-------------------------------------------------------------------------------------
BLD_TOP := $(shell sh -c pwd)

include make/gpo.mk

LIB_NAME = optimizer
LIB_VERSION = 1.640
## ----------------------------------------------------------------------
## top level variables; only used in this makefile
## ----------------------------------------------------------------------

# additional MAKE flags for automated builds
AUTO_MAKE_FLAGS = --no-print-directory -s

# default build arch; overridden by PULSE
BLD_ARCH = matrix

# components to build; each has their own Makefile with default target 'all'
SUBDIRS = libnaucrates libgpdbcost libgpopt server

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
	@$(MAKE) subtargets complexity

complexity: $(OBJDIR_GENERAL)
	@PATH=$(PMCCABE):$$PATH ./make/complexity.sh . $(OBJDIR_GENERAL); ./make/function_owners.sh . $(OBJDIR_GENERAL)

$(OBJDIR_GENERAL):
	@mkdir -p $(OBJDIR_GENERAL)

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
	echo $(ARCH_BIT_ALL)
	@for b in $(ARCH_BIT_ALL); do \
		echo $(BLD_TOP); \
		BLD_ARCH=$$( GPOS_BIT=$$b $(BLD_TOP)/make/platform_defines.sh ); \
		cd make/dependencies; \
		(umask 002; ANT_OPTS="-Djavax.net.ssl.trustStore=$(BLD_TOP)/make/dependencies/cacerts" /opt/releng/apache-ant/bin/ant -DBLD_ARCH=$${BLD_ARCH} resolve); \
		RET_VAL=$$?; \
		if [ $$RET_VAL -ne 0 ] ; then \
			exit $$RET_VAL; \
		fi; \
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

# -------------------------------------------------------------------------
# Target to install GPDB udfs. Currently, this only works on Mac OSX 
# and Linux
# -------------------------------------------------------------------------

GPOPTUTILS_NAMESPACE = gpoptutils

copy_libs_prereq:
	@ if [ "${GPHOME}" = "" ]; then \
		echo "Please source greenplum_path.sh. GPHOME must be set."; \
		exit 1; \
	fi
	echo "Using GPDB installed in ${GPHOME}";
	@ if [ "$(UNAME)" = "Darwin" ]; then \
		if [ "$(ARCH_FLAGS)" != "-m32" ]; then \
			echo "Can only install 32 bit version of UDF on Mac OS X"; \
			exit 1; \
		fi \
	fi

ifdef ($(BLD_ARCH))
ifneq ("osx105_x86" , "$(BLD_ARCH)")
	@echo "Can only use Mac OS X build architecture"
	@exit 1
endif
endif

PGDATABASE := template1
OPTIMIZER := /opt/releng/tools/emc/optimizer/$(LIB_VERSION)/$(shell GPOS_BIT=$(ARCH_BIT) $(BLD_TOP)/make/platform_defines.sh)

copy_libs: copy_libs_prereq all
	@for b in libnaucrates libgpdbcost libgpopt; do \
		rm -f ${GPHOME}/lib/$$b.$(LDSFX); \
		echo "Copying $$b to ${GPHOME}/lib"; \
		cp $(BLD_TOP)/$$b/$(OBJDIR_DEFAULT)/$$b.$(LDSFX) ${GPHOME}/lib ; \
		echo "Copying $$b to $(OPTIMIZER)/$$b/$(OBJDIR_DEFAULT)"; \
		$(SUDO) cp $(BLD_TOP)/$$b/$(OBJDIR_DEFAULT)/$$b.$(LDSFX) $(OPTIMIZER)/$$b/$(OBJDIR_DEFAULT); \
	done

copy_headers: copy_libs_prereq all
	@for b in libnaucrates libgpdbcost libgpopt; do \
		echo "Copying $$b headers to $(OPTIMIZER)/$$b/include"; \
		$(SUDO) rm -rf $(OPTIMIZER)/$$b/include ; \
		$(SUDO) cp -r $$b/include $(OPTIMIZER)/$$b/include ; \
	done

copy_all: SUDO = sudo
copy_all: copy_libs copy_headers

copy_all_no_sudo: SUDO =
copy_all_no_sudo: copy_libs copy_headers

install_udfs:
	sed -e 's,%%CURRENT_DIRECTORY%%,${GPHOME}/lib,g; s,%%GPOPTUTILS_NAMESPACE%%,${GPOPTUTILS_NAMESPACE},g; s,%%EXT%%,$(LDSFX),g' scripts/load.sql.in > /tmp/load.sql
	psql $(PGDATABASE) -a -f /tmp/load.sql

##-------------------------------------------------------------------------------------
## Automated Ivy Publishing support is disabled by default and will
## generally be executed from a triggered pulse build.  This will
## be executed:
##   * If the make variable PUBLISH equals "true".  This is
##     explicitely set via a triggered pulse build.
##-------------------------------------------------------------------------------------

publish_check:
ifeq ("$(PUBLISH)", "true")
	@echo "Publishing $(LIB_NAME) ($(LIB_VERSION)) to Ivy repsository"
	@$(MAKE) publish
else
	@echo "Publishing to Ivy repository is disabled.  Set the Pulse build property PUBLISH to true to update Ivy repository."
endif

make_tarballs:
	@(BUILD_STAGE=`basename $${PULSE_BASE_DIR}`; \
	  TARFILE=`pwd`/targzs/$(LIB_NAME)-devel-$${BUILD_STAGE}-${LIB_VERSION}.targz; \
	  pushd /opt/releng/build; \
	  find . -type f -print | grep $${PULSE_PROJECT}/$${BUILD_STAGE} | grep -v server | grep -v "/doc/" | tar -zcf $${TARFILE} --files-from - ; \
	  popd; \
	 )
	@(BLD_ARCH=$$( GPOS_BIT=$(ARCH_BIT) $(BLD_TOP)/make/platform_defines.sh ); \
	  mkdir -p tmp/$${BLD_ARCH} \
	           tmp/$${BLD_ARCH}/libgpopt \
	           tmp/$${BLD_ARCH}/libgpdbcost \
	           tmp/$${BLD_ARCH}/libnaucrates \
	           tmp/$${BLD_ARCH}/scripts; \
	  cp -r libgpopt/include \
	        tmp/$${BLD_ARCH}/libgpopt; \
	  cp -r libgpopt/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libgpopt; \
	  cp -r libgpdbcost/include \
	        tmp/$${BLD_ARCH}/libgpdbcost; \
	  cp -r libgpdbcost/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libgpdbcost; \
	  cp -r libnaucrates/include \
	        tmp/$${BLD_ARCH}/libnaucrates; \
	  cp -r libnaucrates/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libnaucrates; \
	  pushd tmp; \
	  tar zcf ../targzs/$(LIB_NAME)-$${BLD_ARCH}-${LIB_VERSION}.targz ./$${BLD_ARCH}; \
	  popd; \
	  rm -rf tmp; \
	 )

publish: make_tarballs
	## Disabling publishing of devel artifacts. (espine1)
	@(for ivyfile in ivy.xml; do \
		if [ -f "targzs/$${ivyfile}" ]; then \
				java -Xmx512M -Djavax.net.ssl.trustStore=$(BLD_TOP)/make/dependencies/cacerts \
				     -jar /opt/releng/apache-ant/lib/ivy-2.2.0.jar \
				     -settings targzs/ivysettings.xml \
				     -ivy targzs/$${ivyfile} \
				     -publish public \
				     -publishpattern "targzs/[artifact]-[revision].[ext]" \
				     -revision ${LIB_VERSION} \
				     -status release \
				     -overwrite; \
		fi; \
	done; \
	)


make_tarballs_local:
	@(BLD_ARCH=$$( GPOS_BIT=$(ARCH_BIT) $(BLD_TOP)/make/platform_defines.sh ); \
	  mkdir -p tmp/$${BLD_ARCH} \
	           tmp/$${BLD_ARCH}/libgpopt \
	           tmp/$${BLD_ARCH}/libgpdbcost \
	           tmp/$${BLD_ARCH}/libnaucrates \
	           tmp/$${BLD_ARCH}/scripts; \
	  cp -r libgpopt/include \
	        tmp/$${BLD_ARCH}/libgpopt; \
	  cp -r libgpopt/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libgpopt; \
	  cp -r libgpdbcost/include \
	        tmp/$${BLD_ARCH}/libgpdbcost; \
	  cp -r libgpdbcost/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libgpdbcost; \
	  cp -r libnaucrates/include \
	        tmp/$${BLD_ARCH}/libnaucrates; \
	  cp -r libnaucrates/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libnaucrates; \
	  pushd tmp; \
	  tar zcf ../targzs/$(LIB_NAME)-$${BLD_ARCH}-${LIB_VERSION}.targz ./$${BLD_ARCH}; \
	  popd; \
	  rm -rf tmp; \
	 )

publish_local:  make_tarballs_local
	@(for ivyfile in ivy.xml; do \
		if [ -f "targzs/$${ivyfile}" ]; then \
				java -Xmx512M -jar /opt/releng/apache-ant/lib/ivy-2.2.0.jar \
				     -settings targzs/ivysettings.xml \
				     -ivy targzs/$${ivyfile} \
				     -publish local \
				     -publishpattern "targzs/[artifact]-[revision].[ext]" \
				     -revision ${LIB_VERSION} \
				     -status release \
				     -overwrite; \
		fi; \
	done; \
	)
