MODULE_big = hive_connector
EXTENSION = hive_connector
DATA = hive_connector--1.0.sql
OBJS = hive_connector.o hive_helper.o config_parser.o strings_util.o libhms/src/hmsclient.o
PG_CPPFLAGS = -Wno-unused-variable -DHAVE_NETINET_IN_H -DHAVE_INTTYPES_H -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux
SHLIB_LINK := $(LDFLAGS) -lyaml

ifdef USE_PGXS
PGXS := $(shell pg_config --pgxs)
include $(PGXS)
else
subdir = gpcontrib/hive_connector/native
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
