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

arrow::Status orcReadRecordBatch::addColOrCreateRecordBatch(std::shared_ptr<arrow::Field> field_id, std::shared_ptr<arrow::Array> column, int nrows, int batch_index)
{
	std::shared_ptr<arrow::RecordBatch> newrbatch;
	if (out->num_columns() == 0)
	{
		arrow::FieldVector fields = {field_id};
		arrow::ArrayVector arrays = {column};
		std::shared_ptr<arrow::Schema> schema = std::make_shared<arrow::Schema>(fields, std::make_shared<arrow::KeyValueMetadata>());
		newrbatch = arrow::RecordBatch::Make(schema, nrows, arrays);
	}
	else 
	{
		ARROW_ASSIGN_OR_RAISE(newrbatch, out->AddColumn(batch_index, field_id, column));
	}
	out = newrbatch;
	return arrow::Status::OK();
}

arrow::Status orcReadRecordBatch::recordBatchAddColumn(int mpp_index, int batch_index, std::string partitionkey, Datum mpp_datum, int nrows)
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
			addColOrCreateRecordBatch(field_id, columns, nrows, batch_index);
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
			addColOrCreateRecordBatch(field_id, columns, nrows, batch_index);
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
			addColOrCreateRecordBatch(field_id, columns, nrows, batch_index);
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
			addColOrCreateRecordBatch(field_id, columns, nrows, batch_index);
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
			addColOrCreateRecordBatch(field_id, columns, nrows, batch_index);
			break;
		}
		case NUMERICOID: {
			char* conVal = DatumGetCString(DirectFunctionCall1(numeric_out, mpp_datum));
			arrow::Numeric128 value;
			ARROW_RETURN_NOT_OK(arrow::Numeric128::FromString(conVal, &value));
			arrow::Numeric128Builder numeric128Builder(arrow::numeric128());
			for (int i = 0; i < nrows; i++)
			{
				numeric128Builder.Append(value);
			}
			std::shared_ptr<arrow::Array> columns;
			std::shared_ptr<arrow::Field> field;
			field = arrow::field(partitionkey.c_str(), arrow::numeric128());
			ARROW_ASSIGN_OR_RAISE(columns, numeric128Builder.Finish());
			addColOrCreateRecordBatch(field, columns, nrows, batch_index);
			break;
		}
		case TEXTOID: {
			char* conVal = DatumGetCString(DirectFunctionCall1(textout, mpp_datum));
			arrow::StringBuilder stringBuilder;
			for (int i = 0; i < nrows; i++)
			{
				stringBuilder.Append(conVal, strlen(conVal));
			}
			std::shared_ptr<arrow::Array> columns;
			std::shared_ptr<arrow::Field> field;
			field = arrow::field(partitionkey.c_str(), arrow::utf8());
			ARROW_ASSIGN_OR_RAISE(columns, stringBuilder.Finish());
			addColOrCreateRecordBatch(field, columns, nrows, batch_index);
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
			addColOrCreateRecordBatch(field, columns, nrows, batch_index);
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
				if (!options.includes_columns[defaultMap[i]])
				{
					continue;
				}
				std::string columnName = tupdesc->attrs[defaultMap[i]].attname.data;
				Datum partitionvalue = ExecEvalConst2(defaultExprs[i], NULL, NULL);
				recordBatchAddColumn(defaultMap[i], out->num_columns(), columnName, partitionvalue, tupleIndex);
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
	schemaColMap.clear();
	for (int child = 0; child < size; ++child)
	{
		if (attrs_used.find(child) == attrs_used.end())
		{
			continue;
		}
		std::shared_ptr<arrow::DataType> elemtype;
		arrow::adapters::orc::GetArrowTypeHdw(fileReader.readInterface.type->getSubtype(child), &elemtype);
		std::string name = fileReader.readInterface.type->getFieldName(child);
		fields.push_back(field(name, elemtype));
		schemaColMap.push_back(child);
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
			fileReader.readRecordBatch(pool, schema, &out, schemaColMap);
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
