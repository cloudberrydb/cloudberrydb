#ifndef HUDI_TASK_READER_H
#define HUDI_TASK_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/utils.h"

struct ExternalTableMetadata;

typedef struct HudiTaskReader
{
	Reader       *dataReader;
	FileScanTask *fileScanTask;
	int           taskId;
	ExternalTableMetadata *tableOptions;
	bool         *attrUsed;
	List         *partitionDatums;
} HudiTaskReader;

Reader *createHudiTaskReader(void *args);
bool hudiTaskReaderNext(Reader *reader, InternalRecord *record);
void hudiTaskReaderClose(Reader *reader);

#endif // HUDI_TASK_READER_H
