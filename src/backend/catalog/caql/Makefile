
subdir = src/backend/catalog/caql
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = caqlanalyze.o catquery.o gram.o

FLEXFLAGS = -CF

include $(top_srcdir)/src/backend/common.mk

ifneq ($(findstring -logquery, $(caql_logquery_FLAGS)),)
override CPPFLAGS := $(CPPFLAGS) -DCAQL_LOGQUERY
endif

gram.o: $(srcdir)/scan.c

$(srcdir)/gram.h: $(srcdir)/gram.c ;

$(srcdir)/gram.c: gram.y
ifdef BISON
	$(BISON) -d $(BISONFLAGS) -o $@ $<
else
	@$(missing) bison $< $@
endif

$(srcdir)/scan.c: scan.l
ifdef FLEX
	$(FLEX) $(FLEXFLAGS) -o'$@' $<
	# This is due to flex bug around unused variable yyg in <= 2.5.35
	$(PERL) -pi'' -e 's/^.* depending upon options\. \*\/$$/$$& \n\n(void) yyg;/' $@
else
	@$(missing) flex $< $@
endif

caql_locktest_FLAGS := -lockcheck -readlock \
-holdlock \
-logquery \
-lwl=pg_type,pg_proc,pg_namespace,pg_operator,pg_opclass,pg_amop,pg_am,pg_amproc

# NOTE:
# to turn on caql query logging, do "make caql_logquery_FLAGS=-logquery"
# to filter duplicate caql callers, do "make caql_logquery_FLAGS=-logquery_hash"

test: 
	echo $(caql_logquery_FLAGS) 
	echo $(caql_locktest_FLAGS) 

clean:
	rm -f SUBSYS.o $(OBJS) gram.c gram.h scan.c scan.c
