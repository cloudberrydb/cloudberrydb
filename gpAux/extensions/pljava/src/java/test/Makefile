#-------------------------------------------------------------------------
# Copyright (c) 2004, 2005 TADA AB - Taby Sweden
# Distributed under the terms shown in the file COPYRIGHT
# found in the root folder of this project or at
# http://eng.tada.se/osprojects/COPYRIGHT.html
#
# @author Thomas Hallgren
#-------------------------------------------------------------------------
NAME 			:= test
JAVADOCTITLE	:= 'PL/Java test API Specification'

include $(MODULEROOT)/Makefile.global

all: $(JARFILE)

$(JARFILE): .timestamp
	$(JAR) cf $@ .
