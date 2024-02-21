#include "avro_block_reader.h"
#include "avro_block_reader_c.h"
#include <string>

void *
create_avro_block_reader(MemoryContext mcxt, void *contentBuffer,  void *schemaBuffer)
{
	std::string errorMessage;
	BaseFileReader *reader = NULL;

	try
	{
		reader = new AvroBlockReader(mcxt, (char *) contentBuffer, (char *) schemaBuffer);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to create avro block reader: %s", errorMessage.c_str());

	return reader;
}

void avro_block_open(void *reader,
					 List *columnDesc,
					 bool *attrUsed,
					 int64_t contentBufferLength,
					 int64_t schemaBufferLength)
{
	std::string errorMessage;
	BaseFileReader *reader_ = (BaseFileReader *) reader;

	try
	{
		reader_->open(columnDesc, attrUsed, contentBufferLength, schemaBufferLength);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
		elog(ERROR, "failed to open avro block file: %s", errorMessage.c_str());
}

void avro_block_close(void *reader)
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
		elog(ERROR, "failed to close avro block file: %s", errorMessage.c_str());
}

bool avro_block_next(void *reader, InternalRecord *record)
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
		elog(ERROR, "failed to iterator avro block file: %s", errorMessage.c_str());
	
	return result;
}
