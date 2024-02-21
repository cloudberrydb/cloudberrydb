#ifndef HUDI_DELTALOG_FILTER_H
#define HUDI_DELTALOG_FILTER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "src/provider/common/utils.h"
#include <gopher/gopher.h>

struct ExternalTableMetadata;

typedef struct DeltaLogFilter
{
	Reader         base;
	Reader        *dataReader;
	bool           readLogs;
	ExternalTableMetadata *tableOptions;
	HudiMergedLogfileRecordReader *deltaSet;
} DeltaLogFilter;

DeltaLogFilter *
createDeltaLogFilter(MemoryContext mcxt,
					 List *datafileDesc,
					 bool *attrUsed,
					 Reader *dataReader,
					 gopherFS gopherFilesystem,
					 List *deltaLogs,
					 const char *instantTime,
					 ExternalTableMetadata *tableOptions);

#endif // HUDI_DELTALOG_FILTER_H
