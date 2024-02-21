#ifndef FILE_READER_H
#define FILE_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils.h"
#include <gopher/gopher.h>

typedef struct FormatReader
{
	const char *formatName;
	void *(*Create) (MemoryContext mcxt, void *filenameOrStream, void *extraArg);
	void (*Open) (void *reader, List *columnDesc, bool *attrUsed, int64_t beginOffset, int64_t endOffset);
	bool (*Next) (void *reader, InternalRecord *record);
	void (*Close) (void *reader);
} FormatReader;

typedef struct FileReader
{
	Reader        base;
	bool          isFileStream;
	FileFragment *dataFile;
	void         *dataReader;
	FormatReader *formatReader;
} FileReader;

FileReader *
createFileReader(MemoryContext mcxt,
				 List *columnDesc,
				 bool *attrUsed,
				 bool isFileStream,
				 FileFragment *dataFile,
				 void *extraArg,
				 int64_t beginOffset,
				 int64_t endOffset);

#endif // FILE_READER_H
