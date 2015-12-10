#-------------------------------------------------------------------------------------
#
# Copyright (C) 2011 EMC Corp.
#
# @doc: Makefile utilities
#
#
#-------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------
# general settings
#-------------------------------------------------------------------------------------

# different available build types
BLD_TYPE_ALL = debug opt

# different architectures
ARCH_BIT_ALL = GPOS_32BIT GPOS_64BIT

# default settings
BLD_TYPE = debug
ARCH_BIT = GPOS_64BIT
BLD_VERSION = 0

#-------------------------------------------------------------------------------------
# dependent modules
#
# NOTE: Dependent project module version is kept in $(BLD_TOP)/make/dependencies/ivy.xml
#-------------------------------------------------------------------------------------

GREP_SED_VAR = $(BLD_TOP)/make/dependencies/ivy.xml | sed -e 's|\(.*\)rev="\(.*\)" conf\(.*\)|\2|'

PMCCABE_VER = $(shell grep "pmccabe"    $(GREP_SED_VAR))

PMCCABE = /opt/releng/tools/pa-risc_linux/pmccabe/$(PMCCABE_VER)/$(shell GPOS_BIT=GPOS_32BIT $(BLD_TOP)/make/platform_defines.sh)

#-------------------------------------------------------------------------------------
# default target
#-------------------------------------------------------------------------------------

all:

#-------------------------------------------------------------------------------------
# machine and OS properties

UNAME = $(shell uname)
UNAME_P = $(shell uname -p)

ARCH_OS = GPOS_$(UNAME)
ARCH_CPU = GPOS_$(UNAME_P)

UNAME_ALL = $(UNAME).$(UNAME_P)

#-------------------------------------------------------------------------------------
# tool chain

CPP = g++
LD = g++
CC = gcc

SHELL = bash
SCP = scp
SSH = ssh

PWD = $(shell pwd)
TAR = gtar

P4 = p4

#-------------------------------------------------------------------------------------
# compiler flags

CFLAGS_WARN = -Wall -Werror -Wextra -pedantic-errors -Wno-variadic-macros -Wconversion

CFLAGS_debug = -g3 -DGPOS_DEBUG
CFLAGS_opt = -O3 -fno-omit-frame-pointer -g3

CFLAGS_TYPE = $(CFLAGS_$(BLD_TYPE))

ifeq ($(ARCH_BIT), GPOS_32BIT)
ARCH_FLAGS = -m32
FORCED_MARCH = -march=i686
else
ARCH_FLAGS = -m64
endif

CFLAGS_DL = -fPIC

ifeq (Linux, $(UNAME))
CFLAGS_DL := $(CFLAGS_DL) -rdynamic
endif

# During migration to CMake build system, assume all builds using legacy
# Makefiles use a compiler with GCC-style atomic builtins.
CFLAGS_ATOMICS = -DGPOS_GCC_FETCH_ADD_32 -DGPOS_GCC_FETCH_ADD_64 -DGPOS_GCC_CAS_32 -DGPOS_GCC_CAS_64 $(FORCED_MARCH)

CFLAGS_ARCH = $(ARCH_FLAGS) -D$(ARCH_BIT) -D$(ARCH_CPU) -D$(ARCH_OS)

CFLAGS = $(CFLAGS_WARN) $(CFLAGS_DL) $(CFLAGS_ARCH) $(CFLAGS_ATOMICS) $(CFLAGS_TYPE) $(CFLAGS_EXTRA) $(CFLAGS_INLINE)

OBJDIR_DEFAULT = .obj.$(UNAME_ALL)$(ARCH_FLAGS).$(BLD_TYPE)

#-------------------------------------------------------------------------------------
# linker flags

# shared lib support -- version and compat version to be provided by the including Makefile
ifeq (Darwin, $(UNAME))
	LDLIBFLAGS = -dynamiclib -flat_namespace -undefined dynamic_lookup -current_version $(LIB_VERSION).$(BLD_VERSION) -compatibility_version $(LIB_COMPAT_VERSION) $(LDLIBS) $(LDFLAGS)
	LDSFX = dylib
else
	LDLIBFLAGS = -shared
	LDSFX = so
endif

LDLIBS = -pthread -ldl -lm

# add real-time lib for Solaris builds
ifeq ($(UNAME), SunOS)
LDLIBS := $(LDLIBS) -lrt -lsocket
endif

LDLIBS := $(LDLIBS) $(LDLIBS_EXTRA) 

#-------------------------------------------------------------------------------------
# targets and definitions
#-------------------------------------------------------------------------------------

OBJDIR = $(OBJDIR_DEFAULT)
OBJDIR_GENERAL = .obj.general

SOURCES = $(wildcard *.cpp) $(wildcard *.c)
OBJECTS = $(patsubst %.c, $(OBJDIR)/%.o, $(patsubst %.cpp,$(OBJDIR)/%.o,$(SOURCES)))

OBJ_ORDER = -r

# all objects on this level or below;
# sort objects to link '_api.o' last - static constructors need to be called before 
# calling the library initialization function (tagged with __attribute__((constructor)))  
ALL_OBJS = $(shell find . -name '*.o' | grep "$(OBJDIR)" | sort $(OBJ_ORDER))

DEPS = $(patsubst %.o,%.d,$(OBJECTS))
-include $(DEPS)

#-------------------------------------------------------------------------------------
# targets, rules & dependencies


# dependencies
objects: $(SOURCES)
targets: $(OBJDIR) $(OBJECTS)

debug opt: 
	$(MAKE) all BLD_TOP=$(BLD_TOP) BLD_TYPE=$@

objs:
	@$(MAKE) subtargets targets

lib:	subtargets $(OBJDIR)
	$(MAKE) $(OBJDIR)/$(LIB_NAME).$(LDSFX)

subtargets:
	@for d in $(SUBDIRS); do $(MAKE) --directory=$$d all || exit ; done

purge:
	find . -name \.obj\.\* | xargs rm -rf
	find . -name core | xargs rm -rf
	find . -name *~ | xargs rm -rf
	rm -f gpo.*tgz


#-------------------------------------------------------------------------------------
# rules

# basic compiler commandline; shared by C and C++ compilations below
CC_CMD = -c $(CFLAGS) $(INCDIRS) -MMD -MP -MF $(@:%.o=%.d) $< -o $@

$(OBJDIR)/%.o: %.cpp 
	@$(RM) $@
	$(CPP) $(CC_CMD)

$(OBJDIR)/%.o: %.c
	@$(RM) $@
	$(CC) -std=gnu99 $(CC_CMD)

$(OBJDIR)/%$(LDSFX): $(ALL_OBJS)
	$(CPP) $(ARCH_FLAGS) $(LDLIBFLAGS) -o $(PWD)/$@ $^ $(LDFLAGS) $(LDLIBS)

$(OBJDIR):
	@mkdir $@


#-------------------------------------------------------------------------------------
# setup debug build environment
#-------------------------------------------------------------------------------------

setup:
	rm -rf /opt/releng/build/${PULSE_PROJECT}/$(shell basename ${PULSE_BASE_DIR})
	mkdir -p /opt/releng/build/${PULSE_PROJECT}/$(shell basename ${PULSE_BASE_DIR})
	cp -r ${PULSE_BASE_DIR} /opt/releng/build/${PULSE_PROJECT}

#-------------------------------------------------------------------------------------
# debug targets
#-------------------------------------------------------------------------------------

print:
	@cd $(BLD_TOP)
	@echo "======================================================================"
	@echo "  BLD_TOP ..... : $(BLD_TOP)"
	@echo "  PWD ......... : $(shell pwd)"
	@echo "  BLD_TYPE .... : $(BLD_TYPE)"
	@echo "  BLD_VERSION . : $(BLD_VERSION)"
	@echo "  SOURCES ..... : $(SOURCES)"
	@echo "  ARCH_BIT .... : $(ARCH_BIT)"
	@echo "  OBJECTS ..... : $(OBJECTS)"
	@echo "  SUBDIRS ..... : $(SUBDIRS)"
	@echo "  ALL_OBJS .... : $(ALL_OBJS)"
	@echo "  ARCH_CPU .... : $(ARCH_CPU)"
	@echo "  ARCH_FLAGS .. : $(ARCH_FLAGS)"
	@echo "  OBJDIR ...... : $(OBJDIR)"
	@echo "  CFLAGS ...... : $(CFLAGS)"
	@echo "  LDFLAGS ..... : $(LDFLAGS)"
	@echo "  LDLIBPATH ... : $(LDLIBPATH)"
	@echo "  BUILD_TYPE .. : $(BUILD_TYPE)"
	@echo "  UNAME ....... : $(UNAME)"
	@echo "  UNAME_P ..... : $(UNAME_P)"
	@echo "  PWD ......... : $(PWD)"
	@echo "  DEPS ........ : $(DEPS)"
	@echo "  GCC ......... : $(shell gcc --version)"
	@echo "======================================================================"

#EOF

