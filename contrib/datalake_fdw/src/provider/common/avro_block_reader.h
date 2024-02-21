#ifndef AVRO_BLOCK_READER_H
#define AVRO_BLOCK_READER_H

#include <memory>
#include <vector>

#include <avro.h>
#include "base_reader.h"

class AvroBlockReader : public BaseFileReader
{
private:
	char *contentBuffer_;
	char *schemaBuffer_;
	int   contentBufferPos_;
	int   lastRecordLength_;
	avro_schema_t dataSchema_;
	avro_value_iface_t *schemaIface_;
	avro_reader_t reader_;
	bool  isRecordInited;
	avro_value_t record_;
	std::vector<int> rowGroups_;

public:
	AvroBlockReader(MemoryContext rowContext,
					char *contentBuffer,
					char *schemaBuffer);
	~AvroBlockReader();
	void open(List *columnDesc, bool *attrUsed, int64_t contentBufferLlength, int64_t schemaBufferLength);
	void close();

protected:
	Datum readPrimitive(const TypeInfo &typInfo, bool &isNull);
	void createMapping(List *columnDesc, bool *attrUsed);
	bool readNextRowGroup();
	void decodeRecord();
	void prepareRowGroup();
	avro_value_t *extractField(avro_value_t *container, avro_value_t *field);
};

#endif // AVRO_BLOCK_READER_H
