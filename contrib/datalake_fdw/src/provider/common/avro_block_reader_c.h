#ifndef AVRO_BLOCK_READER_C_H
#define AVRO_BLOCK_READER_C_H

#include "postgres.h"
#include <gopher/gopher.h>

struct List;
struct InternalRecord;

#ifdef __cplusplus
extern "C" {
#endif

void *create_avro_block_reader(MemoryContext mcxt, void *contentBuffer,  void *schemaBuffer);
void avro_block_open(void *reader,
					 List *columnDesc,
					 bool *attrUsed,
					 int64_t contentBufferLength,
					 int64_t schemaBufferLength);
void avro_block_close(void *reader);
bool avro_block_next(void *reader, InternalRecord *record);

#ifdef __cplusplus
}
#endif

#endif // AVRO_BLOCK_READER_C_H
