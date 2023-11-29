# source files directory
REGRESS_DIR=src/test/regress
VECHEADERS = src/include
BACKENDS = src/backend

ARROW = $(BACKENDS)/utils/arrow
ARROWOBJS = $(ARROW)/decimal.o $(ARROW)/transfuncs.o $(ARROW)/wrapper.o $(ARROW)/vecfuncs.o \
			$(ARROW)/fmgr.o $(ARROW)/options.o
PLAN = $(BACKENDS)/optimizer
PLANOBJS = $(PLAN)/planner_vec.o
MISC = $(BACKENDS)/utils/misc
MISCOBJS = $(MISC)/guc_vec.o
VHEAPOBJS = $(BACKENDS)/utils/vheap/vecheap.o
HOOKOBJS = $(BACKENDS)/hook/executor.o $(BACKENDS)/hook/explain.o
EXECUTOR = $(BACKENDS)/vecexecutor
EXECUTOROBJS = $(EXECUTOR)/execSlot.o $(EXECUTOR)/execUtils.o $(EXECUTOR)/execTuples.o  \
			   $(EXECUTOR)/execMain.o $(EXECUTOR)/execScan.o $(EXECUTOR)/nodeSeqscan.o  $(EXECUTOR)/nodeMotion.o $(EXECUTOR)/nodeWindowAgg.o \
			   $(EXECUTOR)/nodeSort.o $(EXECUTOR)/nodeAgg.o $(EXECUTOR)/nodeLimit.o $(EXECUTOR)/nodeResult.o $(EXECUTOR)/execAmi.o \
			   $(EXECUTOR)/nodeHash.o $(EXECUTOR)/nodeHashjoin.o $(EXECUTOR)/nodeAppend.o $(EXECUTOR)/nodeSequence.o  $(EXECUTOR)/nodeSubqueryscan.o \
			   $(EXECUTOR)/nodeMaterial.o $(EXECUTOR)/nodeShareInputScan.o $(EXECUTOR)/nodeNestloop.o $(EXECUTOR)/nodeAssertOp.o \
			   $(EXECUTOR)/nodeForeignscan.o

AMOBJS = $(BACKENDS)/utils/am/am_vec.o
ACCESS = $(BACKENDS)/access
AOCSOBJS = $(ACCESS)/vecaocs/datumstreamblock.o $(ACCESS)/vecaocs/aocs.o
COMMONOBJS = $(ACCESS)/common/printtup_vec.o
CDB = $(BACKENDS)/cdb
MOTIONOBJS = $(CDB)/vecmotion/tupser.o $(CDB)/vecmotion/cdbmotion.o
CDBHASHJS = $(CDB)/veccdbhash.o
SORT = $(BACKENDS)/utils/sort
SORTOBJS = $(SORT)/vecsort.o $(SORT)/tuplestore.o

OBJS = main.o 
OBJS += $(VHEAPOBJS)
OBJS += $(ARROWOBJS)
OBJS += $(AOCSOBJS)
OBJS += $(COMMONOBJS)
OBJS += $(AMOBJS)
OBJS += $(HOOKOBJS)
OBJS += $(PLANOBJS)
OBJS += $(MISCOBJS)
OBJS += $(EXECUTOROBJS)
OBJS += $(MOTIONOBJS)
OBJS += $(SORTOBJS)
OBJS += $(CDBHASHJS)

# add arrow.so and headers
PG_CFLAGS += $(shell pg_config --cflags)
PG_CFLAGS += $(shell pg_config --cppflags)
PG_CFLAGS += $(shell PKG_CONFIG_PATH=${GPHOME}/lib/pkgconfig/:${PKG_CONFIG_PATH} pkg-config --cflags arrow-glib gandiva-glib)
PG_LDFLAGS += $(shell PKG_CONFIG_PATH=${GPHOME}/lib/pkgconfig/:${PKG_CONFIG_PATH} pkg-config --libs arrow-glib gandiva-glib)
PG_CFLAGS += -I$(shell pg_config --includedir-server)
PG_LDFLAGS += -L$(shell pg_config --libdir)

# support libpqd
PG_CFLAGS += -I $(shell pg_config --includedir)
PG_LDFLAGS += -I $(shell pg_config --ldflags)

PG_CFLAGS += -I $(VECHEADERS) 

MODULE_big = vectorization
EXTENSION = vectorization
DATA = vectorization--1.0.sql

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PGXS_DIR := $(dir $(PGXS))
PG_REGRESS = $(PGXS_DIR)/../../src/test/regress/pg_regress

all: 
	make -C $(REGRESS_DIR) all

load:
	gpconfig -c session_preload_libraries -v '${GPHOME}/lib/postgresql/${EXTENSION}.so'
	gpstop -u

installcheck: vec_test icw_parallel_test icw_greenplum_test

vec_test:
	mkdir -p data/tpch
	wget https://artifactory.hashdata.xyz/artifactory/list/utility/tpch_data.tar
	tar xf tpch_data.tar -C data/tpch
	rm -f tpch_data.tar
	$(PG_REGRESS) --init-file=init_file --schedule=vectorization_schedule --load-extension=vectorization

icw_parallel_test:
	make -C $(REGRESS_DIR) icw_parallel_test
icw_greenplum_test:
	make -C $(REGRESS_DIR) icw_greenplum_test
icw_test:
	make -C $(REGRESS_DIR) icw_test
icw_aocs:
	make -C $(REGRESS_DIR) icw_aocs

