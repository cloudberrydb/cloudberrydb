subdir=src/backend/utils/etcd_lib
top_builddir=../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=etcdlib

include $(top_srcdir)/src/backend/mock.mk
LDFLAGS := -luuid $(LDFLAGS)
CFLAGS := -o0 $(CFLAGS)

etcdlib.t: $(MOCK_DIR)/backend/utils/error/elog_mock.o \
	$(MOCK_DIR)/backend/access/hash/hashpage_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o \
	$(MOCK_DIR)/backend/storage/ipc/shmem_mock.o \
	$(MOCK_DIR)/backend/storage/lmgr/lwlock_mock.o
