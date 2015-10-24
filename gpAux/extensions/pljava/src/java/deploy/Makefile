#-------------------------------------------------------------------------
# Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
# Distributed under the terms shown in the file COPYRIGHT
# found in the root folder of this project or at
# http://eng.tada.se/osprojects/COPYRIGHT.html
#
# @author Thomas Hallgren
#-------------------------------------------------------------------------
NAME			:= deploy
JAVADOCTITLE	:= 'PL/Java deployer API Specification'

include $(MODULEROOT)/Makefile.global

all: $(JARFILE)

$(JARFILE): .timestamp
	$(JAR) cmf $(SRCDIR)/META-INF/manifest.txt $@ .
