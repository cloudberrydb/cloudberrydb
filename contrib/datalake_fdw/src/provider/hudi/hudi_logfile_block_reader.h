#ifndef HUDI_LOGFILE_BLOCK_READER_H
#define HUDI_LOGFILE_BLOCK_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "src/provider/common/utils.h"
#include <gopher/gopher.h>

typedef struct HudiLogFileReader
{
	MemoryContext  mcxt;
	gopherFS       gopherFilesystem;
	gopherFile     gopherFile;
	char          *fileName;
	int64_t        offset;
} HudiLogFileReader;

typedef enum HudiLogBlockType
{
	COMMAND_BLOCK = 0,
	DELETE_BLOCK,
	CORRUPTED_BLOCK,
	AVRO_BLOCK,
	HFILE_BLOCK,
	PARQUET_BLOCK,
	CDC_BLOCK
} HudiLogBlockType;

typedef enum HeaderMetadataType
{
	INSTANT_TIME = 0,
	TARGET_INSTANT_TIME,
	SCHEMA,
	COMMAND_BLOCK_TYPE,
	COMPACTED_BLOCK_TIMES,
	METADATA_TYPE_MAX
} HeaderMetadataType;

typedef struct HudiMetadataElem
{
	char *value;
} HudiMetadataElem;

typedef struct HudiLogFileBlock
{
	HudiLogBlockType type;
} HudiLogFileBlock;

typedef struct HudiLogFileCorruptlock
{
	HudiLogBlockType	type;
	int					length;
	char			   *content;
} HudiLogFileCorruptBlock;

typedef struct HudiLogFileDataBlock
{
	HudiLogBlockType	type;
	int					length;
	char			   *content;
	HudiMetadataElem   *header;
} HudiLogFileDataBlock;

const char * logBlockGetTypeName(HudiLogFileBlock *block);
void freeLogBlock(HudiLogFileBlock *block);
const char *logBlockGetInstantTime(HudiLogFileBlock *block);
const char *logBlockGetTargetInstantTime(HudiLogFileBlock *block);
const char *logBlockGetSchema(HudiLogFileBlock *block);
HudiLogFileReader *createHudiLogFileReader(MemoryContext mcxt, gopherFS gopherFilesystem, char *filename);
void hudiLogFileOpen(HudiLogFileReader *reader);
bool hudiLogFileNext(HudiLogFileReader *reader, HudiLogFileBlock **logBlock);
void hudiLogFileClose(HudiLogFileReader *reader);

#endif // HUDI_LOGFILE_BLOCK_READER_H
