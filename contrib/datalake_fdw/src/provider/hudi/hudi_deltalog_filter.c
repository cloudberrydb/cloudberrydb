#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "catalog/pg_type.h"
#include "gopher/gopher.h"
#include "src/provider/common/file_reader.h"
#include "hudi_merged_logfile_record_reader.h"
#include "hudi_deltalog_filter.h"

static bool deltaLogFilterNext(Reader *filter, InternalRecord *record);
static void deltaLogFilterClose(Reader *filter);
static bool logFilterNext(DeltaLogFilter *filter, InternalRecord *record);
static bool mergeFilterNext(DeltaLogFilter *filter, InternalRecord *record);

static Reader methods = {
	NULL,
	deltaLogFilterNext,
	deltaLogFilterClose,
};

DeltaLogFilter *
createDeltaLogFilter(MemoryContext mcxt,
					 List *datafileDesc,
					 bool *attrUsed,
					 Reader *dataReader,
					 gopherFS gopherFilesystem,
					 List *deltaLogs,
					 const char *instantTime,
					 ExternalTableMetadata *tableOptions)
{
	DeltaLogFilter *filter = palloc0(sizeof(DeltaLogFilter));

	filter->base = methods;
	filter->dataReader = dataReader;
	filter->tableOptions = tableOptions;
	filter->readLogs = false;
	filter->deltaSet = createMergedLogfileRecordReader(mcxt,
													   datafileDesc,
													   attrUsed,
													   instantTime,
													   gopherFilesystem,
													   deltaLogs,
													   tableOptions);
	if (dataReader == NULL)
		elog(DEBUG1, "create hudi log only filter");
	else
		elog(DEBUG1, "create hudi merge on read filter");

	return filter;
}

static bool
deltaLogFilterNext(Reader *filter, InternalRecord *record)
{
	DeltaLogFilter *deltaLogFilter = (DeltaLogFilter *) filter;

	if (deltaLogFilter->dataReader == NULL)
		return logFilterNext(deltaLogFilter, record);

	return mergeFilterNext(deltaLogFilter, record);
}

static void
deltaLogFilterClose(Reader *filter)
{
	DeltaLogFilter *deltaLogFilter = (DeltaLogFilter *) filter;

	if (deltaLogFilter->dataReader)
		deltaLogFilter->dataReader->Close(deltaLogFilter->dataReader);

	mergedLogfileRecordReaderClose(deltaLogFilter->deltaSet);

	pfree(deltaLogFilter);

	elog(DEBUG1, "close hudi delta log filter");
}

static bool
logFilterNext(DeltaLogFilter *filter, InternalRecord *record)
{
	return mergedLogfileRecordReaderNext(filter->deltaSet, record);
}

static bool
mergeFilterNext(DeltaLogFilter *filter, InternalRecord *record)
{
	int  i;
	bool exist;
	bool isDeleted;
	InternalRecord *newRecord;

	while(!filter->readLogs && filter->dataReader->Next(filter->dataReader, record))
	{
		exist = mergedLogfileContains(filter->deltaSet, record, &newRecord, &isDeleted);
		if (exist)
		{
			if (isDeleted)
				continue;

			for(i = 0; i < filter->deltaSet->recordDesc->nColumns; i++)
			{
				record->values[i] = newRecord->values[i];
				record->nulls[i] = newRecord->nulls[i];
			}
		}

		return true;
	}

	filter->readLogs = true;
	return logFilterNext(filter, record);
}
