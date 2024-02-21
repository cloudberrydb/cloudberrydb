// #include "postgres.h"
#include "gopher/gopher.h"
#include "base_reader.h"
#include "access/tupdesc.h"
#include "utils.h"

BaseFileReader::BaseFileReader(MemoryContext rowContext)
	: curGroup_(-1), curRow_(0), numRows_(0), rowContext_(rowContext)
{}

BaseFileReader::~BaseFileReader()
{}

void
BaseFileReader::populateRecord(InternalRecord *record)
{
	bool   isNull;
	size_t size = typeMap_.size();

	decodeRecord();

	for (size_t attr = 0; attr < size; attr++)
	{
		TypeInfo &typInfo = typeMap_[attr];
		if (typInfo.columnIndex_ < 0)
		{
			record->nulls[attr] = true;
			continue;
		}

		isNull = false;
		record->values[attr] = readPrimitive(typInfo, isNull);
		record->nulls[attr] = isNull;
	}

	record->position = rowPositions_[curGroup_] + curRow_;
}

bool
BaseFileReader::next(InternalRecord *record)
{
	MemoryContext oldContext;

	if (curRow_ >= numRows_)
	{
		do
		{
			if (!readNextRowGroup())
				return false;
		}
		while (!numRows_);
	}

	MemoryContextReset(rowContext_);

	oldContext = MemoryContextSwitchTo(rowContext_);
	populateRecord(record);
	MemoryContextSwitchTo(oldContext);

	curRow_++;

	return true;
}
