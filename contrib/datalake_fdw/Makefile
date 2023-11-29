# contrib/datalake_fdw/Makefile

MODULE_big = datalake_fdw
OBJS = 	src/datalake_fdw.o \
		src/datalake_def.o \
		src/datalake_fragment.o \
		src/datalake_option.o \
		src/common/dataBufferArray.o \
		src/common/fileSystemWrapper.o \
		src/common/fileSystem.o \
		src/common/logicalType.o \
		src/common/readPolicy.o \
		src/common/rewrLogical.o \
		src/common/makeHash.o \
		src/common/util.o \
		src/common/partition_selector.o \
		src/dlproxy/deparse.o \
		src/dlproxy/filters.o \
		src/dlproxy/headers.o \
		src/dlproxy/hive.o \
		src/dlproxy/hudi.o \
		src/dlproxy/iceberg.o \
		src/dlproxy/libchurl.o \
		src/dlproxy/protocol.o \
		src/dlproxy/uriparser.o \
		src/dlproxy/utils.o \
		src/provider/parquet/read/parquetRead.o \
		src/provider/parquet/read/parquetInputStream.o \
		src/provider/parquet/read/parquetFileReader.o \
		src/provider/parquet/write/parquetWrite.o \
		src/provider/parquet/write/parquetOutputStream.o \
		src/provider/parquet/write/parquetFileWriter.o \
		src/provider/orc/read/orcInputStream.o \
		src/provider/orc/read/orcReadDeltaFile.o \
		src/provider/orc/read/orcReadInterface.o \
		src/provider/orc/read/orcFileReader.o \
		src/provider/orc/read/orcRead.o \
		src/provider/orc/read/orcReadRecordBatch.o \
		src/provider/orc/write/orcWriter.o \
		src/provider/archive/read/archiveFileRead.o \
		src/provider/archive/read/archiveRead.o \
		src/provider/archive/write/archiveFileWrite.o \
		src/provider/archive/write/archiveWrite.o \
		src/provider/providerWrapper.o \
		src/provider/provider.o \
		src/hdw_gopher_cache/hdw_gopher_cache.o


SHLIB_LINK += -lgopher -lparquet -lorc -larchive -ljansson -larrow

EXTENSION = datalake_fdw
DATA = datalake_fdw--1.0.sql

REGRESS = hive_connecter_orc_read hive_connecter_orc_vec_read hive_connecter_parquet_read orc_read parquet_read iresearch_orc_test iresearch_orc_vec_test hive_connector


PG_CPPFLAGS += -fPIC
PG_CFLAGS += -I $(shell pg_config --includedir)
PG_LDFLAGS += -I $(shell pg_config --ldflags)

ifdef USE_PGXS
PG_CONFIG = pg_config

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/datalake_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
