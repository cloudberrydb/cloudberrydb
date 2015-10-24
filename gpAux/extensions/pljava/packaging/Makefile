#-------------------------------------------------------------------------
# Copyright (c) 2003, 2004 TADA AB - Taby Sweden
# Distributed under the terms shown in the file COPYRIGHT.
#
# @author Thomas Hallgren
#-------------------------------------------------------------------------
include $(PGXS)

SS_VERSION := $(subst ., ,$(subst devel,.devel,$(subst beta,.beta,$(subst RC,.RC,$(VERSION)))))
PGSQL_MAJOR_VER = $(word 1,$(SS_VERSION))
PGSQL_MINOR_VER = $(word 2,$(SS_VERSION))
DISTRIB := distrib

ifdef USE_GCJ
	GCJEXTRA=-gcj-
else
	GCJEXTRA=-
endif

RELNAME := pljava-$(host_tuple)$(GCJEXTRA)pg$(PGSQL_MAJOR_VER).$(PGSQL_MINOR_VER)-$(PLJAVA_VERSION)
SRCNAME := pljava-src-$(PLJAVA_VERSION)
PKGNAME := pljava-$(PLJAVA_VERSION)

release:
	@-mkdir -p $(RELNAME)
	@-rm -f $(RELNAME)/*
	@find .	\(				\
		   -name '*.jar'	\
		-o -name '*.so'		\
		-o -name '*.dll'	\
		\) -a -not -path ./$(RELNAME)'/*' -exec cp {} $(RELNAME) \;
	@cp ../src/sql/*.sql $(RELNAME)
	@$(TAR) cf $(RELNAME)/docs.tar docs
	(cd $(RELNAME); $(TAR) zcf ../distrib/$(RELNAME).tar.gz .)

source_tarball:
	@-mkdir -p distrib
	@-mkdir -p $(PKGNAME)
	@-rm -rf $(PKGNAME)/*
	@(cd ..; find . \( \
		   -name CVS \
		-o -name bin \
		-o -name build \
		-o -name .cvsignore \
		\) -prune -o \( -type f -exec cp --parent {} build/$(PKGNAME) \; \) )
	@$(TAR) zcf $(DISTRIB)/$(SRCNAME).tar.gz $(PKGNAME)

JAR				:= jar

PUBLIC_ROOT		:= classes/pljava
PUBLIC_CLASSES	:= $(wildcard $(PUBLIC_ROOT)/org/postgresql/pljava/*.*)
PUBLIC_REL_CLASSES = $(foreach var,$(PUBLIC_CLASSES),$(subst $(PUBLIC_ROOT)/,,$(var)))

PUBLIC_SRC_ROOT	:= ../src/java/pljava
PUBLIC_SRCS		:= $(wildcard $(PUBLIC_SRC_ROOT)/org/postgresql/pljava/*.*)
PUBLIC_REL_SRCS = $(foreach var,$(PUBLIC_SRCS),$(subst $(PUBLIC_SRC_ROOT)/,,$(var)))

PUBLIC_NAME		:= pljava-public-$(PLJAVA_VERSION)
PUBLIC_BUNDLE	:= distrib/$(PUBLIC_NAME)-bundle.jar
JIRA_DESC		:= distrib/jira-desc.txt
BUNDLEDIR		:= maven_bundle
PUBLIC_JARFILE	:= $(PUBLIC_NAME).jar
PUBLIC_SRC_JARFILE	:= $(PUBLIC_NAME)-src.jar

maven_bundle: $(PUBLIC_BUNDLE) $(JIRA_DESC)

$(PUBLIC_BUNDLE): $(BUNDLEDIR)/LICENSE.txt $(BUNDLEDIR)/project.xml $(BUNDLEDIR)/$(PUBLIC_JARFILE) $(BUNDLEDIR)/$(PUBLIC_SRC_JARFILE)
	@(cd $(BUNDLEDIR); $(JAR) cf ../$@ *)

$(BUNDLEDIR)/LICENSE.txt: $(PROJDIR)/COPYRIGHT
	@-mkdir -p $(BUNDLEDIR)
	@cp $< $(BUNDLEDIR)/LICENSE.txt

$(BUNDLEDIR)/project.xml: $(PROJDIR)/packaging/project.xml
	@-mkdir -p $(BUNDLEDIR)
	@sed -e 's/@PLJAVA_VERSION@/'$(PLJAVA_VERSION)'/' $< > $@

$(BUNDLEDIR)/$(PUBLIC_JARFILE): $(PUBLIC_CLASSES)
	@-mkdir -p $(BUNDLEDIR)
	@(cd $(PUBLIC_ROOT); $(JAR) cf ../../$@ $(PUBLIC_REL_CLASSES))

$(BUNDLEDIR)/$(PUBLIC_SRC_JARFILE): $(PUBLIC_SRCS)
	@-mkdir -p $(BUNDLEDIR)
	@(cd $(PUBLIC_SRC_ROOT); $(JAR) cf ../../../build/$@ $(PUBLIC_REL_SRCS)) 

$(JIRA_DESC): $(PROJDIR)/packaging/jira-desc.txt
	@sed -e 's/@PLJAVA_VERSION@/'$(PLJAVA_VERSION)'/' $< > $@

