#ifndef ICEBERG_POSITION_FILTER_H
#define ICEBERG_POSITION_FIlTER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/utils.h"

typedef struct PositionFilter
{
	Reader            base;
	void             *deletesSet;
	bool              isEmptySet;
	Reader           *dataReader;
	void             *sortedMerge;
	int64             nextPosition;
	bool              isEmptyStream;
	MemoryContext	  mcxt;
} PositionFilter;

PositionFilter *
createPositionFilter(MemoryContext readerMcxt,
					 Reader *dataReader,
					 gopherFS gopherFilesystem,
					 char *dataFilePath,
					 List *deletes);

#endif // ICEBERG_POSITION_FILTER_H
