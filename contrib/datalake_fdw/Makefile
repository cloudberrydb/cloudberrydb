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
		src/provider/common/config.o \
		src/provider/common/gopher_random_file.o \
		src/provider/common/base_reader.o \
		src/provider/common/common.o \
		src/provider/common/utils.o \
		src/provider/common/parquet_reader_c.o \
		src/provider/common/parquet_reader.o \
		src/provider/common/avro_block_reader.o \
		src/provider/common/avro_block_reader_c.o \
		src/provider/common/file_reader.o \
		src/provider/common/delete_bitmap.o \
		src/provider/common/delete_bitmap_c.o \
		src/provider/common/sorted_merge.o \
		src/provider/common/sorted_merge_c.o \
		src/provider/common/kryo.o \
		src/provider/common/kryo_input.o \
		src/provider/common/row_reader.o \
		src/provider/iceberg/iceberg_equality_filter.o \
		src/provider/iceberg/iceberg_task_reader.o \
		src/provider/iceberg/iceberg_position_filter.o \
		src/provider/iceberg/iceberg_read.o \
		src/provider/hudi/hudi_task_reader.o \
		src/provider/hudi/hudi_deltalog_filter.o \
		src/provider/hudi/hudi_logfile_block_reader.o \
		src/provider/hudi/hudi_merged_logfile_record_reader.o \
		src/provider/hudi/hudi_read.o \
		src/provider/avro/read/avroBlockReader.o \
		src/provider/avro/read/avroRead.o \
		src/provider/avro/write/avroWriter.o \
		src/provider/avro/write/avroWrite.o \
		src/provider/providerWrapper.o \
		src/provider/provider.o \
		src/hdw_gopher_cache/hdw_gopher_cache.o


SHLIB_LINK += -lgopher -lparquet -lorc-hdw -larchive -lavrocpp -ljansson -larrow  -lavro -lcurl
PG_CPPFLAGS = -DENABLE_NLS -DSNAPPY_CODEC_AVAILABLE

EXTENSION = datalake_fdw
DATA = datalake_fdw--1.0.sql

ifdef ICEBERG_HUDI_TEST
REGRESS = iceberg_read hudi_read
else
REGRESS = hive_connecter_orc_read hive_connecter_orc_vec_read hive_connecter_parquet_read \
          hive_connecter_avro_read hive_connector orc_read parquet_read avro_read iresearch_orc_test iresearch_orc_vec_test
endif

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
