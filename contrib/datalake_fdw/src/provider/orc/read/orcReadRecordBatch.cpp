#include "orcReadRecordBatch.h"
#include "arrow/c/bridge.h"
#include "arrow/adapters/orc/adapter_util.h"
#include <list>
#include <cassert>
#include <sstream>

extern "C" {
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
}

namespace Datalake {
namespace Internal {


void orcReadRecordBatch::createHandler(void *sstate)
{
	initParameter(sstate);
	initializeColumnValue();
	createPolicy();
	deltaFile.readDeleteDeltaLists((void*)scanstate->options,
		readPolicy.deleteDeltaLists, setStreamWhetherCache(options));
	pool = arrow::default_memory_pool();
	readNextGroup();

}

void orcReadRecordBatch::destroyHandler()
{
	tupleIndex = 0;
	releaseResources();
}

void orcReadRecordBatch::restart()
{
	tupleIndex = 0;
	stripeIndex = 0;
	last = false;
	fileReader.closeORCReader();
	readPolicy.reset();
	initializeColumnValue();
	createPolicy();
	deltaFile.reset();
	deltaFile.readDeleteDeltaLists((void*)scanstate->options,
		readPolicy.deleteDeltaLists, setStreamWhetherCache(options));
	readNextGroup();
}

arrow::Status orcReadRecordBatch::recordBatchAddColumn(int mpp_index, std::string partitionkey, Datum mpp_datum, int nrows)
{
	Oid typeOid = tupdesc->attrs[mpp_index].atttypid;
	switch (typeOid)
	{
		case INT2OID: {
			int16_t conVal = Int16GetDatum(mpp_datum);
			std::shared_ptr<arrow::Array> columns;
			arrow::Int16Builder builder;
			std::vector<int16_t> vec;
			for (int i = 0; i < nrows; i++)
			{
				vec.push_back(conVal);
			}
			std::shared_ptr<arrow::Field> field_id;
			field_id = arrow::field(partitionkey.c_str(), arrow::int16());
			ARROW_RETURN_NOT_OK(builder.AppendValues(vec));
			ARROW_ASSIGN_OR_RAISE(columns, builder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field_id, columns));
			out = newrbatch;

			break;
		}
		case INT4OID: {
			int32_t conVal = DatumGetInt64(mpp_datum);
			std::shared_ptr<arrow::Array> columns;
			arrow::Int32Builder builder;
			std::vector<int32_t> vec;
			for (int i = 0; i < nrows; i++)
			{
				vec.push_back(conVal);
			}
			std::shared_ptr<arrow::Field> field_id;
			field_id = arrow::field(partitionkey.c_str(), arrow::int32());
			ARROW_RETURN_NOT_OK(builder.AppendValues(vec));
			ARROW_ASSIGN_OR_RAISE(columns, builder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field_id, columns));
			out = newrbatch;
			break;
		}
		case INT8OID: {
			int64_t conVal = DatumGetInt64(mpp_datum);
			std::shared_ptr<arrow::Array> columns;
			arrow::Int64Builder builder;
			std::vector<int64_t> vec;
			for (int i = 0; i < nrows; i++)
			{
				vec.push_back(conVal);
			}
			std::shared_ptr<arrow::Field> field_id;
			field_id = arrow::field(partitionkey.c_str(), arrow::int64());
			ARROW_RETURN_NOT_OK(builder.AppendValues(vec));
			ARROW_ASSIGN_OR_RAISE(columns, builder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field_id, columns));
			out = newrbatch;
			break;
		}
		case FLOAT4OID: {
			float conVal = DatumGetFloat4(mpp_datum);
			std::shared_ptr<arrow::Array> columns;
			arrow::FloatBuilder builder;
			std::vector<float> vec;
			for (int i = 0; i < nrows; i++)
			{
				vec.push_back(conVal);
			}
			std::shared_ptr<arrow::Field> field_id;
			field_id = arrow::field(partitionkey.c_str(), arrow::float32());
			ARROW_RETURN_NOT_OK(builder.AppendValues(vec));
			ARROW_ASSIGN_OR_RAISE(columns, builder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field_id, columns));
			out = newrbatch;
			break;
		}

		case FLOAT8OID: {
			double conVal = DatumGetFloat8(mpp_datum);
			std::shared_ptr<arrow::Array> columns;
			arrow::DoubleBuilder builder;
			std::vector<double> vec;
			for (int i = 0; i < nrows; i++)
			{
				vec.push_back(conVal);
			}
			std::shared_ptr<arrow::Field> field_id;
			field_id = arrow::field(partitionkey.c_str(), arrow::float64());
			ARROW_RETURN_NOT_OK(builder.AppendValues(vec));
			ARROW_ASSIGN_OR_RAISE(columns, builder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field_id, columns));
			out = newrbatch;
			break;
		}
		case NUMERICOID: {
			char* conVal = DatumGetCString(DirectFunctionCall1(numeric_out, mpp_datum));
			arrow::Decimal128 value;
			int32_t precision = 0;
			int32_t scale = 0;
			ARROW_RETURN_NOT_OK(arrow::Decimal128::FromString(conVal, &value, &precision, &scale));

			arrow::Decimal128Builder decimal128Builder(arrow::decimal(precision - scale, scale));
			for (int i = 0; i < nrows; i++)
			{
				decimal128Builder.Append(value);
			}
			std::shared_ptr<arrow::Array> columns;
			std::shared_ptr<arrow::Field> field;
			field = arrow::field(partitionkey.c_str(), arrow::decimal(precision - scale, scale));
			ARROW_ASSIGN_OR_RAISE(columns, decimal128Builder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field, columns));
			out = newrbatch;
			break;
		}
		case TEXTOID: {
			char* conVal = DatumGetCString(DirectFunctionCall1(textout, mpp_datum));
			arrow::BinaryBuilder binaryBuilder;
			for (int i = 0; i < nrows; i++)
			{
				binaryBuilder.Append(conVal, strlen(conVal));
			}
			std::shared_ptr<arrow::Array> columns;
			std::shared_ptr<arrow::Field> field;
			field = arrow::field(partitionkey.c_str(), arrow::binary());
			ARROW_ASSIGN_OR_RAISE(columns, binaryBuilder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field, columns));
			out = newrbatch;
			break;
		}
		case BPCHAROID: {
			char* conVal = DatumGetCString(DirectFunctionCall1(textout, mpp_datum));
			arrow::BinaryBuilder binaryBuilder;
			for (int i = 0; i < nrows; i++)
			{
				binaryBuilder.Append(conVal, strlen(conVal));
			}
			std::shared_ptr<arrow::Array> columns;
			std::shared_ptr<arrow::Field> field;
			field = arrow::field(partitionkey.c_str(), arrow::binary());
			ARROW_ASSIGN_OR_RAISE(columns, binaryBuilder.Finish());
			std::shared_ptr<arrow::RecordBatch> newrbatch;
			ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(mpp_index, field, columns));
			out = newrbatch;
			break;
		}
		default:
			elog(ERROR, "does not support column:\"%s\" typeOid:\"%u\" as a partition key",
					partitionkey.c_str(), typeOid);
	}
	return arrow::Status::OK();
}

int64_t orcReadRecordBatch::read(void **recordBatch)
{
nextPartition:

	if (readNextRecordBatch(recordBatch))
	{
		if (scanstate->options->hiveOption->hivePartitionKey != NULL)
		{
			AttrNumber numDefaults = this->nDefaults;
			int *defaultMap = this->defMap;
			ExprState **defaultExprs = this->defExprs;

			for (int i = 0; i < numDefaults; i++)
			{
				std::string columnName = tupdesc->attrs[defaultMap[i]].attname.data;
				Datum partitionvalue = ExecEvalConst2(defaultExprs[i], NULL, NULL);
				recordBatchAddColumn(defaultMap[i], columnName, partitionvalue, tupleIndex);
			}
		}
		arrow::ExportRecordBatch(*out.get(), &c_batch);
		*recordBatch = &c_batch;
		return 1;
	}

	if (!isLastPartition(scanstate))
	{
		restart();
		goto nextPartition;
	}
	return 0;
}

bool orcReadRecordBatch::readNextRecordBatch(void **recordBatch)
{
	while((getFileState() == FILE_OPEN) && !last)
	{
		if (!getRecordBatch(recordBatch))
		{
			if (!readNextGroup())
			{
				/* read over */
				last = true;
				return false;
			}
		}
		else
		{
			return true;
		}
	}
	return false;
}

void orcReadRecordBatch::ReadSchema(std::shared_ptr<arrow::Schema>* out_schema)
{
	int size = fileReader.readInterface.type->getSubtypeCount();
	std::vector<std::shared_ptr<arrow::Field>> fields;
	for (int child = 0; child < size; ++child)
	{
		if (attrs_used.find(child) == attrs_used.end())
		{
			continue;
		}
		std::shared_ptr<arrow::DataType> elemtype;
		arrow::adapters::orc::GetArrowType(fileReader.readInterface.type->getSubtype(child), &elemtype);
		std::string name = fileReader.readInterface.type->getFieldName(child);
		fields.push_back(field(name, elemtype));
	}
	/* Comment out the getMetadataKeys. Not used at this stage */
	//const std::list<std::string> keys = fileReader.readInterface.reader->getMetadataKeys();
	auto metadata = std::make_shared<arrow::KeyValueMetadata>();
	// for (const auto& key : keys)
	// {
	// 	metadata->Append(key, fileReader.readInterface.reader->getMetadataValue(key));
	// }
	*out_schema = std::make_shared<arrow::Schema>(std::move(fields), std::move(metadata));
}

bool orcReadRecordBatch::getRecordBatch(void** recordBatch)
{
	while (fileReader.readInterface.batch != NULL)
	{
		if (tupleIndex < (int64_t)fileReader.readInterface.batch->numElements)
		{
			std::shared_ptr<arrow::Schema> schema;
			ReadSchema(&schema);
			fileReader.readRecordBatch(pool, schema, &out);
			tupleIndex = fileReader.readInterface.batch->numElements;
			return true;
		}
		tupleIndex = 0;
		return false;
	}
	tupleIndex = 0;
	return false;
}

}
}
