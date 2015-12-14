subdir=src/backend/utils/mdver
top_builddir=../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=mdver_global_generation mdver_local_generation mdver_inv_translator

include $(top_builddir)/src/backend/mock.mk

mdver_global_generation.t: \
	$(MOCK_DIR)/backend/storage/ipc/shmem_mock.o \
	$(MOCK_DIR)/backend/utils/mdver/mdver_utils_mock.o

mdver_local_generation.t: \
	$(MOCK_DIR)/backend/utils/cache/inval_mock.o \
	$(MOCK_DIR)/backend/utils/mdver/mdver_utils_mock.o \
	$(MOCK_DIR)/backend/utils/mdver/mdver_global_generation_mock.o

mdver_inv_translator.t: \
	$(MOCK_DIR)/backend/utils/cache/inval_mock.o \
	$(MOCK_DIR)/backend/utils/mdver/mdver_utils_mock.o \
	$(MOCK_DIR)/backend/catalog/catalog_mock.o
