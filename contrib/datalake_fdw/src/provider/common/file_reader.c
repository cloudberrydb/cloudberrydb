#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "gopher/gopher.h"
#include "file_reader.h"
#include "parquet_reader_c.h"
#include "avro_block_reader_c.h"

static bool fileReaderNext(Reader *reader, InternalRecord *record);
static void fileReaderClose(Reader *reader);

static Reader methods = {
	NULL,
	fileReaderNext,
	fileReaderClose,
};

static FormatReader parquetReader = {
	"parquet",
	create_parquet_reader,
	parquet_open,
	parquet_next,
	parquet_close
};

static FormatReader orcReader = {
	"orc",
	NULL,
	NULL,
	NULL,
	NULL
};

static FormatReader avroReader = {
	"avro",
	NULL,
	NULL,
	NULL,
	NULL
};

static FormatReader avroBlockReader = {
	"avro block",
	create_avro_block_reader,
	avro_block_open,
	avro_block_next,
	avro_block_close
};

FileReader *
createFileReader(MemoryContext mcxt,
				 List *columnDesc,
				 bool *attrUsed,
				 bool isFileStream,
				 FileFragment *dataFile,
				 void *extraArg,
				 int64_t beginOffset,
				 int64_t endOffset)
{
	FileReader *reader = palloc0(sizeof(FileReader));

	reader->base = methods;
	reader->dataFile = dataFile;
	reader->isFileStream = isFileStream;

	switch (dataFile->format)
	{
		case PARQUET:
			reader->formatReader = &parquetReader;
			break;
		case ORC:
			reader->formatReader = &orcReader;
			break;
		case AVRO:
			reader->formatReader = &avroReader;
			break;
		case AVRO_FILE_BLOCK:
			reader->formatReader = &avroBlockReader;
			break;
		default:
			elog(ERROR, "unknown file type: %d", dataFile->format);
	}

	if (reader->formatReader->Create == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("file format \"%s\" is not supported", reader->formatReader->formatName)));

	reader->dataReader = reader->formatReader->Create(mcxt, dataFile->filePath, extraArg);
	reader->formatReader->Open(reader->dataReader, columnDesc, attrUsed, beginOffset, endOffset);

	return reader;
}

static bool
fileReaderNext(Reader *reader, InternalRecord *record)
{
	FileReader *fileReader = (FileReader *) reader;
	return fileReader->formatReader->Next(fileReader->dataReader, record);
}

static void
fileReaderClose(Reader *reader)
{
	FileReader *fileReader = (FileReader *) reader;

	fileReader->formatReader->Close(fileReader->dataReader);
	if (fileReader->isFileStream)
		pfree(fileReader->dataFile->filePath);

	pfree(fileReader->dataFile);
	pfree(fileReader);
}
