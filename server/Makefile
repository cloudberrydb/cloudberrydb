#-------------------------------------------------------------------------------------
#
# Copyright (C) 2008 - 2010 Greenplum Inc.
#
# @doc: Makefile for server binary
#
# @author: waasf
#
#-------------------------------------------------------------------------------------

BLD_TOP := ..
include $(BLD_TOP)/make/gpo.mk

#-------------------------------------------------------------------------------------
# add gpos, gpopt, gpdbcost, dxl and xerces libraries to pre-defined flags
LDLIBS := $(LDLIBS) -lgpos -lxerces-c -lnaucrates -lgpopt -lgpdbcost
LIBDIRS = $(LIBGPOS_LIBDIR) $(XERCES_LIBDIR) $(BLD_TOP)/libnaucrates/$(OBJDIR) $(BLD_TOP)/libgpopt/$(OBJDIR) $(BLD_TOP)/libgpdbcost/$(OBJDIR)

LDFLAGS := $(foreach DIR, $(LIBDIRS), -L$(DIR))

# manufacture LD_LIBRARY_PATH setting for test target
EMPTY :=
SPACE := $(EMPTY) $(EMPTY)
COLON:= :
LDLIBPATH = $(subst $(SPACE),$(COLON), $(LIBDIRS))

ifeq "$(TEST_PARAM)" "-x"
TEST_LOG = $(OBJDIR)/test_log_ext.txt
else
TEST_LOG = $(OBJDIR)/test_log.txt
endif

SUBDIRS = src

#-------------------------------------------------------------------------------------
# targets
#-------------------------------------------------------------------------------------

all: subtargets $(OBJDIR)
	$(MAKE) $(OBJDIR)/gpo

$(OBJDIR)/gpo: $(ALL_OBJS)
	$(CPP) $(CFLAGS_BIT) $(CFLAGS) $(ALL_OBJS) -o $@ $(LDFLAGS) $(LDLIBS) $(LDXFLAGS)

test:
ifeq ($(shell uname -s), Darwin)
	DYLD_LIBRARY_PATH=$$DYLD_LIBRARY_PATH$(LDLIBPATH) $(OBJDIR)/gpo $(TEST_PARAM) &> $(TEST_LOG)
else
	LD_LIBRARY_PATH=$$LD_LIBRARY_PATH$(LDLIBPATH) $(OBJDIR)/gpo $(TEST_PARAM) &> $(TEST_LOG)
endif
	@echo "*** unittests: arch = $(ARCH_BIT) $(arch_cpu) type = $(BLD_TYPE) ***"
	@echo "`grep OK $(TEST_LOG) | wc -l` OK"
	@echo "`grep FAILED $(TEST_LOG) | wc -l` FAILED"
	@echo "`grep FAILED $(TEST_LOG)`"
	@echo "*** end of unittests ***"
	@echo

# eof
