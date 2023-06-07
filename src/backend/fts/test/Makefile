subdir=src/backend/fts
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=ftsmessagehandler

include $(top_srcdir)/src/backend/mock.mk

ftsmessagehandler.t: \
    $(MOCK_DIR)/backend/storage/lmgr/lwlock_mock.o \
    $(MOCK_DIR)/backend/replication/gp_replication_mock.o \
    $(MOCK_DIR)/backend/libpq/pqformat_mock.o \
    $(MOCK_DIR)/backend/libpq/pqcomm_mock.o \
    $(MOCK_DIR)/backend/tcop/dest_mock.o \
    $(MOCK_DIR)/backend/access/transam/xlog_mock.o \
    $(MOCK_DIR)/backend/replication/slot_mock.o \
    $(MOCK_DIR)/backend/access/hash/hash_mock.o \
    $(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o
