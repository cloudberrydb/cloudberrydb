#ifndef DATALAKE_ORCREADRECORDBATCH_H
#define DATALAKE_ORCREADRECORDBATCH_H

#include "arrow/c/abi.h"
#include "orcRead.h"


namespace Datalake {
namespace Internal {


class orcReadRecordBatch : public orcRead
{
public:
	virtual void createHandler(void *sstate);

	virtual int64_t read(void **recordBatch);

	virtual void destroyHandler();

private:

	void restart();

	bool readNextRecordBatch(void **recordBatch);

	bool getRecordBatch(void** recordBatch);

	void ReadSchema(std::shared_ptr<arrow::Schema>* out_schema);

	arrow::Status recordBatchAddColumn(int mpp_index, std::string partitionkey, Datum mpp_datum, int nrows);

	arrow::MemoryPool* pool;
	std::shared_ptr<arrow::RecordBatch> out;
	struct ArrowRecordBatch c_batch{};
};

}
}

#endif
