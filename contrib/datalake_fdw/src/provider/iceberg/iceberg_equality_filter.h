#ifndef ICEBERG_EQUALITY_FILTER_H
#define ICEBERG_EQUALITY_FILTER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils/hsearch.h"
#include "src/provider/common/utils.h"
#include <gopher/gopher.h>

typedef struct EqualityFilter
{
	Reader         base;
	List          *deletesSets;
	Reader        *dataReader;
	MemoryContext  mcxt;
} EqualityFilter;

EqualityFilter *
createEqualityFilter(MemoryContext readerMcxt,
					 List *datafileTupleDesc,
					 Reader *dataReader,
					 gopherFS gopherFilesystem,
					 List *deletes);

#endif // ICEBERG_EQUALITY_FIlTER_H
