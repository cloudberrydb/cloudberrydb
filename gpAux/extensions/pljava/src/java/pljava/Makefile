#-------------------------------------------------------------------------
# Copyright (c) 2004, 2005 TADA AB - Taby Sweden
# Distributed under the terms shown in the file COPYRIGHT
# found in the root folder of this project or at
# http://eng.tada.se/osprojects/COPYRIGHT.html
#
# @author Thomas Hallgren
#-------------------------------------------------------------------------
NAME			:= pljava
JAVADOCTITLE	:= 'PL/Java v$(PLJAVA_MAJOR_VER).$(PLJAVA_MINOR_VER) API Specification'

include $(MODULEROOT)/Makefile.global

SRCDIR_USC	:= $(subst .,^,$(SRCDIR))
mkclsrc		= $(subst ^,.,$(subst .,/,$(1:%=$(SRCDIR_USC)/%^java)))

INTPKG		:= org.postgresql.pljava.internal
JDBCPKG		:= org.postgresql.pljava.jdbc

JNI_CLASSES := \
	$(INTPKG).Backend \
	$(INTPKG).SPI \
	$(INTPKG).AclId \
	$(INTPKG).ErrorData \
	$(INTPKG).Oid \
	$(INTPKG).ExecutionPlan \
	$(INTPKG).JavaWrapper \
	$(INTPKG).LargeObject \
	$(INTPKG).PgSavepoint \
	$(INTPKG).Portal \
	$(INTPKG).Relation \
	$(INTPKG).Session \
	$(INTPKG).SubXactListener \
	$(INTPKG).TriggerData \
	$(INTPKG).Tuple \
	$(INTPKG).TupleDesc \
	$(INTPKG).XactListener \
	$(JDBCPKG).Invocation \
	$(JDBCPKG).SingleRowReader \
	$(JDBCPKG).SQLInputFromChunk \
	$(JDBCPKG).SQLOutputToChunk \
	$(JDBCPKG).SQLInputFromTuple \
	$(JDBCPKG).SQLOutputToTuple

JNISRCS := $(call mkclsrc,$(JNI_CLASSES))

ifdef USE_GCJ
# We include both the pljava_jar.o and the pjlava.jar here although
# pljava doesn't need the latter. Most java compilers will need it
# in order to compile triggers and functions later.
#
JNIHDRS = $(patsubst %,$(JNIDIR)/%.h,$(subst .,_,$(JNI_CLASSES)))
mkcname = $(subst _,.,$(1:$(JNIDIR)/%.h=%))

all: $(OBJDIR)/$(NAME)_jar.o $(JARFILE) $(JNIDIR)/.timestamp

$(OBJDIR)/$(NAME)_jar.o: .timestamp
	@-mkdir -p $(@D)
	@echo $(GCJ) -c -fpic -fjni -o $@ '<java sources>'
	@$(GCJ) -c -fpic -fjni -o $@ $(SRCS)

# gcjh fails to clear its function name cache between files (see
# gcc bugzilla #17575) so we must do each file separately to
# avoid name ambiguities that causes names to be generated with
# parameter type info.
#
$(JNIHDRS): $(JNISRCS)
	@-mkdir -p $(@D)
	@gcjh -jni --classpath=. -d $(@D) $(call mkcname,$@)

# gcjh generated headers don't define final constants (see gcc
# bugzilla #16843) so we must copy our own version of java.sql.Types. 
#
$(JNIDIR)/.timestamp: $(JNIHDRS) .timestamp
	@-mkdir -p $(@D)
	@cp $(PROJDIR)/fixes/gcj/java_sql_Types.h $(@D)
	@touch $@
else
all: $(JARFILE) $(JNIDIR)/.timestamp

# .timestamp waits for javac in $(MODULEROOT)/Makefile.global
$(JNIDIR)/.timestamp: $(JNISRCS) .timestamp
	@-mkdir -p $(@D)
	@echo $(JAVAH) -classpath . -d $(@D) '<jni classes>'
	@$(JAVAH) -classpath . -d $(@D) $(JNI_CLASSES) java.sql.Types
	@touch $@

endif

$(JARFILE): .timestamp
	$(JAR) cf $@ .
