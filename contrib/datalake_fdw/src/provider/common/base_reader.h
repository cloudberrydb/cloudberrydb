#ifndef BASE_READER_H
#define BASE_READER_H

extern "C"
{
#include "postgres.h"
#include "utils/memutils.h"
}

#include <vector>

struct InternalRecord;
struct List;

union ReaderValue
{
	bool    boolValue;
	int32_t int32Value;
	int64_t int64Value;
	float   floatValue;
	double  doubleValue;
};

class BaseFileReader
{
protected:
	struct TypeInfo
	{
		Oid pgTypeId_;
		int typeMod_;
		Oid fileTypeId_;
		int columnIndex_;
	};

	int curGroup_;
	uint32_t curRow_;
	uint32_t numRows_;
	MemoryContext rowContext_;
	std::vector<TypeInfo> typeMap_;
	std::vector<int64_t> rowPositions_;

	virtual Datum readPrimitive(const TypeInfo &typInfo, bool &isNull) = 0;
	virtual bool readNextRowGroup() = 0;
	virtual void createMapping(List *columnDesc, bool *attrUsed) = 0;
	virtual void decodeRecord() = 0;

	void populateRecord(InternalRecord *record);

public:
	BaseFileReader(MemoryContext rowContext);
	virtual ~BaseFileReader() = 0;
	bool next(InternalRecord *record);
	virtual void open(List *columnDesc, bool *attrUsed, int64_t startOffset, int64_t endOffset) = 0;
	virtual void close() = 0;
};

#endif // BASE_READER_H
