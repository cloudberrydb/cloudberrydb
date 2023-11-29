#ifndef DATALAKE_ORCFILEREADER_H
#define DATALAKE_ORCFILEREADER_H

#include "arrow/adapters/orc/adapter.h"
#include "arrow/api.h"
#include "orcReadInterface.h"
#include "src/common/logicalType.h"
#include "src/common/rewrLogical.h"
#include "orcReadDeltaFile.h"


namespace Datalake {
namespace Internal {

class orcFileReader : public ORCLogicalType
{
public:
	orcFileReader()
	{
		state = FILE_UNDEF;
		name = "";
	}

	bool createORCReader(void* opt, std::string fileName, int64_t length, readOption options);

	void resetORCReader();

	void closeORCReader();

	void read(TupleDesc tupdesc, Datum *values, bool *nulls, int index, int columns);

	arrow::Status readRecordBatch(arrow::MemoryPool* pool,
				std::shared_ptr<arrow::Schema>& schema,
				std::shared_ptr<arrow::RecordBatch>* out);

	bool compareToDeleteMap(orcReadDeltaFile &compact, int index);

	fileState getState()
    {
        return state;
    }

	orcReadInterface readInterface;

private:
	Datum readField(Oid typeOid, int rowIndex, const char *columnName, int columnIndex, orc::ColumnVectorBatch *batch);

	readOption options;
	fileState state;
	std::string name;
};

}
}
#endif