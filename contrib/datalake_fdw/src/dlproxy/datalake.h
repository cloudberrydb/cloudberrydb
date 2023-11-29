#ifndef DATALAKE_H
#define DATALAKE_H

#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "src/datalake_def.h"

#define CATALOG_TYPE       "catalog_type"
#define SERVER_NAME	       "server_name"
#define TABLE_IDENTIFIER   "table_identifier"

typedef struct TableFieldDefination
{
	char *fieldName;
	char *fieldTypeName;
	Oid   fieldTypeOid;
	int32 fieldTypeMod1;
	int32 fieldTypeMod2;
} TableFieldDefination;

typedef enum FileContent
{
	DATA = 0,
	POSITION_DELETES,
	EQUALITY_DELETES,
	DELTA_LOG
} FileContent;

typedef enum FileFormat
{
	ORC = 0,
	PARQUET,
	AVRO,
	HFILE,
	HLOG,
	AVRO_FILE_BLOCK
} FileFormat;

typedef struct CombinedScanTask
{
	List *fileTasks;
} CombinedScanTask;

extern List *
get_external_schema(char *profile,
					char *relName,
					char *schemaName,
					List *locations);

extern List *
get_external_fragments(Oid relid,
					   Index relno,
					   List *restrictInfo,
					   List *targetList,
					   List *locations,
					   char* formatType,
					   bool iswritable);

extern List *
parsePartitionResponse(char *buffer, size_t buffer_size);
extern void
freePartitionList(List *partitions);

#endif   /* DATALAKE_H */

