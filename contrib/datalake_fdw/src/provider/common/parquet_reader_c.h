#ifndef PARQUET_READER_C_H
#define PARQUET_READER_C_H

#include <gopher/gopher.h>

struct List;
struct InternalRecord;

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

void *create_parquet_reader(MemoryContext mcxt, void *filePath, void *gopherFilesystem);
void parquet_open(void *reader,
				  List *columnDesc,
				  bool *attrUsed,
				  int64_t beginOffset,
				  int64_t endOffset);
void parquet_close(void *reader);
bool parquet_next(void *reader, InternalRecord *record);

#ifdef __cplusplus
}
#endif

#endif // PARQUET_READER_C_H
