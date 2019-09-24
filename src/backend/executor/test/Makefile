subdir=src/backend/executor
top_builddir=../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=nodeShareInputScan

include $(top_builddir)/src/backend/mock.mk

nodeShareInputScan.t: \
	$(MOCK_DIR)/backend/executor/execUtils_mock.o \
	$(MOCK_DIR)/backend/utils/sort/tuplestorenew_mock.o
