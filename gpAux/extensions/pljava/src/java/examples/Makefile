#-------------------------------------------------------------------------
# Copyright (c) 2004, 2005 TADA AB - Taby Sweden
# Distributed under the terms shown in the file COPYRIGHT
# found in the root folder of this project or at
# http://eng.tada.se/osprojects/COPYRIGHT.html
#
# @author Thomas Hallgren
#-------------------------------------------------------------------------
NAME 			:= examples
JAVADOCTITLE	:= 'PL/Java examples API Specification'
JAVAFLAGS		:= -classpath $(TARGETDIR)/classes/pljava

include $(MODULEROOT)/Makefile.global

all: $(JARFILE)

$(JARFILE): .timestamp \
			$(SRCDIR)/deployment/examples.manifest \
			$(SRCDIR)/deployment/examples.ddr \
			$(SRCDIR)/org/postgresql/example/example.properties
	$(JAR) cmf $(SRCDIR)/deployment/examples.manifest $@ . \
	-C $(SRCDIR) deployment/examples.ddr \
	-C $(SRCDIR) org/postgresql/example/example.properties
