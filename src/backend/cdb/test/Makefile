subdir=src/backend/cdb
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS= cdbdistributedsnapshot

TARGETS += cdbappendonlyxlog

TARGETS += cdbutils

# There are some functions both defined in src/backend/storage/file/fd.c
# and libpgcommon.a which contains object files with FRONTEND defined,
# for use by client applications.
# we should use libpgcommon_srv.a which contains object files without FRONTEND defined,
# for use only by the backend
LIBS := -lpgcommon_srv $(LIBS)

include $(top_srcdir)/src/backend/mock.mk

cdbdistributedsnapshot.t: $(MOCK_DIR)/backend/access/transam/distributedlog_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o

cdbappendonlyxlog.t: \
	$(MOCK_DIR)/backend/storage/file/fd_mock.o \
	$(MOCK_DIR)/backend/access/transam/xlogutils_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o

cdbutils.t: \
	$(MOCK_DIR)/backend/storage/lmgr/lwlock_mock.o \
	$(MOCK_DIR)/backend/catalog/namespace_mock.o \
	$(MOCK_DIR)/backend/access/table/table_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o \
	$(MOCK_DIR)/backend/storage/ipc/shmem_mock.o