#include "parquet_reader_c.h"
#include "parquet_reader.h"

void *
create_parquet_reader(MemoryContext mcxt, void *filePath, void *gopherFilesystem)
{
	std::string errorMessage;
	BaseFileReader *reader = NULL;

	try
	{
		reader = new ParquetReader(mcxt, (char *) filePath, (gopherFS) gopherFilesystem);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to create parquet reader: %s", errorMessage.c_str());

	return reader;
}

void parquet_open(void *reader,
				  List *columnDesc,
				  bool *attrUsed,
				  int64_t startOffset,
				  int64_t endOffset)
{
	std::string errorMessage;
	BaseFileReader *reader_ = (BaseFileReader *) reader;

	try
	{
		reader_->open(columnDesc, attrUsed, startOffset, endOffset);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to open parquet file: %s", errorMessage.c_str());
}

void parquet_close(void *reader)
{
	std::string errorMessage;
	BaseFileReader *reader_ = (BaseFileReader *) reader;

	try
	{
		reader_->close();
		delete reader_;
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to close parquet file: %s", errorMessage.c_str());
}

bool parquet_next(void *reader, InternalRecord *record)
{
	bool result;
	std::string errorMessage;
	BaseFileReader *reader_ = (BaseFileReader *) reader;

	try
	{
		result = reader_->next(record);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to iterator parquet file: %s", errorMessage.c_str());
	
	return result;
}
