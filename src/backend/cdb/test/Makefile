subdir=src/backend/cdb
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=cdbbufferedread \
	cdbbackup \
	cdbfilerep \
	cdbsrlz

include $(top_builddir)/src/backend/mock.mk

cdbfilerep.t: \
	$(MOCK_DIR)/backend/postmaster/fork_process_mock.o \
	$(MOCK_DIR)/backend/utils/mmgr/redzone_handler_mock.o
