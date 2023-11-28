MODULE_big = unionstore
OBJS = \
	$(WIN32RES) \
	file_cache.o \
	libpagestore.o \
	libpqwalproposer.o \
	unionstore.o \
	pagestore_smgr.o \
	relsize_cache.o \
	walproposer.o \
	walproposer_utils.o \
	unionstore_xlog.o \
	accessext/ushash.o \
    accessext/usnbtree.o \
	accessext/usgist.o \
	accessext/usgin.o \
	accessext/usspgist.o \
	accessext/usbrin.o \
	accessext/usbitmap.o

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = $(libpq)

EXTENSION = unionstore
DATA = unionstore--1.0.sql
PGFILEDESC = "unionstore - cloud storage for CloudBerry"

# UnionStore's regression could only running with installcheck.
REGRESS = set_up boolean char name varchar text int2 int4 int8 oid float8 float4 txid \
          numeric bit uuid money enum regproc pg_lsn strings point numerology \
          lseg line path date circle timetz timestamp time timestamptz interval \
          macaddr8 macaddr tstypes regex horology comments xid unicode mvcc \
          index/aocs_sample index/appendonly_sample index/boolean index/case \
		  index/collate index/comments index/copydml index/copyselect \
		  index/create_aggregate index/create_cast \
		  index/create_function_3 index/create_procedure \
		  index/date index/delete index/disable_autovacuum \
		  index/drop_operator index/enable_autovacuum index/enum index/float4 \
		  index/float8 index/generated index/hash_func index/identity \
		  index/infinite_recurse index/init_privs \
		  index/insert index/insert_conflict index/int2 index/int4 index/int8 \
		  index/interval index/line index/lseg index/macaddr index/macaddr8 \
		  index/misc_sanity index/mvcc index/money index/name \
		  index/namespace index/numerology index/object_address index/password \
		  index/path index/pg_lsn index/replica_identity index/regex index/regproc \
		  index/roleattributes index/rowsecurity \
		  index/security_label index/strings index/tstypes \
		  index/select_having index/select_implicit index/timetz index/time \
		  index/text index/txid index/unicode index/timestamp index/typed_table \
		  index/update index/uuid index/vacuum_stats index/varchar index/xid \

REGRESS_OPTS = --init-file=init_file --load-extension=unionstore

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
top_builddir = ../..

all: unionstore.tar.gz

unionstore.tar.gz:
	git clone https://code.hashdata.xyz/cloudberry/unionstore.git $(HOME)/unionstore && \
	rm -rf $(HOME)/unionstore/vendor/unionstore_walredo && \
	git clone https://code.hashdata.xyz/cloudberry/unionstore_walredo.git $(HOME)/unionstore/vendor/unionstore_walredo && \
	cp -r $(top_builddir)/* $(HOME)/unionstore/vendor/postgres-v14/ && \
	cp -r $(top_builddir)/* $(HOME)/unionstore/vendor/postgres-v15/ && \
	cd $(HOME)/unionstore && \
	MAKELEVEL=0 make -sj 12 && \
	tar --exclude='deps' --exclude='build' -czf unionstore.tar.gz pg_install target/release unionstore_deploy.sh && \
	cd - && mv $(HOME)/unionstore/unionstore.tar.gz ./

install: install_unionstore

install_unionstore:
	cp unionstore.tar.gz $(GPHOME)

clean: clean_unionstore

clean_unionstore:
	rm -f $(GPHOME)/unionstore.tar.gz && \
	rm -rf $(HOME)/unionstore && \
	rm -f unionstore.tar.gz

.PHONY: all install clean