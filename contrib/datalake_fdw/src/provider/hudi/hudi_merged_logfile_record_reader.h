#ifndef HUDI_MERGED_LOGFILE_RECORD_READER_H
#define HUDI_MERGED_LOGFILE_RECORD_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "src/provider/common/utils.h"

struct ExternalTableMetadata;

typedef struct HudiMergedLogfileRecordReader
{
	List	   *logfiles;
	List	   *columnDesc;
	List	   *queuedBlocks;
	bool	   *attrUsed;
	const char *instantTime;
	gopherFS	gopherFilesystem;
	MemoryContext mergerMcxt;
	MemoryContext deleteBlockMcxt;
	MemoryContext deleteRowMcxt;
	MemoryContext readerMcxt;
	ExternalTableMetadata *tableOptions;
	MergeProvider *mergeProvider;
} HudiMergedLogfileRecordReader;

HudiMergedLogfileRecordReader *
createMergedLogfileRecordReader(MemoryContext readerMcxt,
								List *columnDesc,
								TupleDesc tupDesc,
								bool *attrUsed,
								const char *instantTime,
								gopherFS gopherFilesystem,
								List *logfiles,
								ExternalTableMetadata *tableOptions);

void performScan(HudiMergedLogfileRecordReader *reader);

void mergedLogfileRecordReaderClose(HudiMergedLogfileRecordReader *reader);

bool mergedLogfileRecordReaderNext(HudiMergedLogfileRecordReader *reader, InternalRecord *record);

bool mergedLogfileContains(HudiMergedLogfileRecordReader *reader,
						   InternalRecord *record,
						   InternalRecord **newRecord,
						   bool *isDeleted);

#endif // HUDI_MERGED_LOGFILE_RECORD_READER_H
