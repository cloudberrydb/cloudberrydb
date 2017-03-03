subdir=src/backend/postmaster
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=syslogger bgwriter

include $(top_builddir)/src/backend/mock.mk
bgwriter.t: $(MOCK_DIR)/backend/storage/lmgr/lwlock_mock.o
