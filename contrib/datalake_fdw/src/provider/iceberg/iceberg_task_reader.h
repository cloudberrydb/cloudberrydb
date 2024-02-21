#ifndef ICEBERG_TASK_READER_H
#define ICEBERG_TASK_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/utils.h"

typedef struct IcebergTaskReader
{
	Reader       *dataReader;
	FileScanTask *fileScanTask;
	int           taskId;
} IcebergTaskReader;

Reader *createIcebergTaskReader(void *args);
bool icebergTaskReaderNext(Reader *reader, InternalRecord *record);
void icebergTaskReaderClose(Reader *reader);

#endif // ICEBERG_TASK_READER_H
