##-------------------------------------------------------------------------------------

LIB_NAME = libgpos
LIB_VERSION = 1.127
LIB_COMPAT_VERSION = 1.0

##-------------------------------------------------------------------------------------

BLD_TOP := ..
include $(BLD_TOP)/make/gpo.mk

SUBDIRS = src

all: lib

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
	  mkdir -p tmp/$${BLD_ARCH}/libgpos; \
	  cp -r ../libgpos/include \
	        tmp/$${BLD_ARCH}/libgpos; \
	  cp -r ../libgpos/.obj.$(UNAME_ALL)$(ARCH_FLAGS).* \
	        tmp/$${BLD_ARCH}/libgpos; \
	  pushd tmp; \
	  tar zcf ../targzs/$(LIB_NAME)-$${BLD_ARCH}-${LIB_VERSION}.targz $${BLD_ARCH}; \
	  popd; \
	  rm -rf tmp; \
	 )

publish: make_tarballs
	@(for ivyfile in ivy.xml ivy-devel.xml; do \
		if [ -f "targzs/$${ivyfile}" ]; then \
			java -Djavax.net.ssl.trustStore=$(BLD_TOP)/make/dependencies/cacerts \
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
