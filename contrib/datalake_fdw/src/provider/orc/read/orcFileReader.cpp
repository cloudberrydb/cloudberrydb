#include "arrow/adapters/orc/adapter_util.h"
#include "orcFileReader.h"
#include <list>
#include <cassert>
#include <sstream>
extern "C"
{
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

#define ORC_FIELD_STR_BUF_LEN (1024)

namespace Datalake {
namespace Internal {

bool orcFileReader::createORCReader(void* opt, std::string fileName, int64_t length, readOption options)
{
	this->options = options;
	state = FILE_OPEN;
	readInterface.setTransactionTable(options.transactionTable);
	return readInterface.createORCReader(opt, fileName, length,
		setStreamWhetherCache(options), options.allocBuffer);
}

void orcFileReader::resetORCReader()
{
	readInterface.batch.reset();
	readInterface.rowReader.reset();
	readInterface.reader.reset();
	if (state != FILE_UNDEF)
	{
		readInterface.deleteORCReader();
	}
	state = FILE_CLOSE;
}

void orcFileReader::closeORCReader()
{
	if (state == FILE_CLOSE)
    {
        return;
    }
	resetORCReader();
	state = FILE_CLOSE;
}

void orcFileReader::read(TupleDesc tupdesc, Datum *values, bool *nulls, int index, int columns)
{
	orc::StructVectorBatch *orcVector = readInterface.getORCStructVectorBatch();
	int fieldsLength = orcVector->fields.size();
	for (int i = 0, j = 0; i < columns; ++i)
	{
		if (!options.includes_columns[i])
		{
			nulls[i] = true;
			if (options.transactionTable)
            {
                /*
                transcation table format
                {"operation":0,
                "originalTransaction":1,
                "bucket":536870912,
                "rowId":0,
                "currentTransaction":1,
                "row":{"id":1,"name":"Jerry","salary":5000}}

                transcation table take the field of "row" save data in rowoption.
                So get all the columns of transcation table. Here is to skip the columns
                that select will not query.
                */
                ++j;
            }
            continue;
		}
		if (j >= fieldsLength || (orcVector->fields[j]->hasNulls && orcVector->fields[j]->notNull[index] == false))
        {
            nulls[i] = true;
            ++j;
            continue;
        }

        values[i] = readField(tupdesc->attrs[i].atttypid, index, tupdesc->attrs[i].attname.data, i, orcVector->fields[j]);
        nulls[i] = false;
        ++j;
	}
	return;
}

arrow::Status orcFileReader::readRecordBatch(arrow::MemoryPool* pool,
				std::shared_ptr<arrow::Schema>& schema,
				std::shared_ptr<arrow::RecordBatch>* out,
				std::vector<int32_t> &schemaColMap)
{
	std::unique_ptr<arrow::RecordBatchBuilder> builder;
	arrow::RecordBatchBuilder::Make(schema, pool, ORC_BATCH_SIZE, &builder);
	orc::StructVectorBatch *orcVector = readInterface.getORCStructVectorBatch();
	for (int i = 0; i < builder->num_fields(); i++)
	{
		RETURN_NOT_OK(arrow::adapters::orc::AppendBatchHdw(readInterface.type->getSubtype(schemaColMap[i]), orcVector->fields[i], 0,
								readInterface.batch->numElements, builder->GetField(i)));
	}
	RETURN_NOT_OK(builder->Flush(out));
	return arrow::Status::OK();
}

Datum orcFileReader::readField(Oid typeOid, int rowIndex, const char *columnName, int columnIndex, orc::ColumnVectorBatch *batch)
{
    auto colType = readInterface.type->getSubtype(columnIndex);
    auto orcColType = colType->getKind();

    if (!checkDataTypeCompatible(typeOid, orcColType))
    {
        std::string column_type = colType->toString().data();
        readInterface.deleteORCReader();
        elog(ERROR,
            "Type Mismatch: data in '%s' is defined as '%s' in ORC file, but in external table "
            "as '%s'. %s",
            columnName, column_type.c_str(), getColTypeName(typeOid).data(), getTypeMappingSupported().data());
        return 0;
    }

    switch (typeOid)
    {
		case BOOLOID:
		{
			orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
			return BoolGetDatum(vec->data[rowIndex] != 0L);
		}
		case INT8OID:
		{
			orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
			return Int64GetDatum(vec->data[rowIndex]);
		}
		case INT4OID:
		{
			orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
			return Int32GetDatum((int32_t)vec->data[rowIndex]);
		}
		case INT2OID:
		{
			orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
			return Int16GetDatum((int16_t)vec->data[rowIndex]);
		}
		case FLOAT4OID:
		{
			orc::DoubleVectorBatch *vec = dynamic_cast<orc::DoubleVectorBatch *>(batch);
			return Float4GetDatum((float)vec->data[rowIndex]);
		}
		case FLOAT8OID:
		{
			orc::DoubleVectorBatch *vec = dynamic_cast<orc::DoubleVectorBatch *>(batch);
			return Float8GetDatum(vec->data[rowIndex]);
		}
		case INTERVALOID:
		{
			// No corresponding data type in orc, needs the source to be string
			orc::StringVectorBatch *vec = dynamic_cast<orc::StringVectorBatch *>(batch);
			size_t size = vec->length[rowIndex];
			if (size > ORC_FIELD_STR_BUF_LEN)
			{
				readInterface.deleteORCReader();
				elog(ERROR, "Field %d length too large.", rowIndex);
			}
			char readBuf[ORC_FIELD_STR_BUF_LEN];
			memset(readBuf, 0, ORC_FIELD_STR_BUF_LEN);
			memcpy(readBuf, vec->data[rowIndex], size);
			return DirectFunctionCall1(interval_in, CStringGetDatum(readBuf));
		}
		case DATEOID:
		{
			orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
			int32_t date = vec->data[rowIndex] + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE);
			return Int32GetDatum(date);

		}
		case TIMEOID:
		{
			// No corresponding data type in orc, needs the source to be string
			orc::StringVectorBatch *vec = dynamic_cast<orc::StringVectorBatch *>(batch);
			size_t size = vec->length[rowIndex];
			if (size > ORC_FIELD_STR_BUF_LEN)
			{
				readInterface.deleteORCReader();
				elog(ERROR, "Field %d length too large.", rowIndex);
			}
			char readBuf[ORC_FIELD_STR_BUF_LEN];
			memset(readBuf, 0, ORC_FIELD_STR_BUF_LEN);
			memcpy(readBuf, vec->data[rowIndex], size);
			return DirectFunctionCall1(time_in, CStringGetDatum(readBuf));
		}
		case TIMESTAMPOID:
		{
			orc::TimestampVectorBatch *vec = dynamic_cast<orc::TimestampVectorBatch *>(batch);
			int64_t second = (int64_t)vec->data[rowIndex];
			int64_t nanosec = (int64_t)vec->nanoseconds[rowIndex];
			int64_t timestamp = 0;
			int64_t micsecs = (second + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) *60*60*24) * 1000000;
			int64_t nano = nanosec/1000;
			timestamp = micsecs + nano;
			return Int64GetDatum(timestamp);

		}
		case NUMERICOID:
		{
			int scale = 0;
			bool sign = true;
			std::string asString;
			if (colType->getPrecision() <= 18)
			{
				orc::Decimal64VectorBatch *d64vec = dynamic_cast<orc::Decimal64VectorBatch *>(batch);
				int64_t *values = d64vec->values.data();
				scale = d64vec->scale;
				int64_t value = values[rowIndex];
				if (value == 0)
					return DirectFunctionCall3(numeric_in, CStringGetDatum("0"), ObjectIdGetDatum(0), Int32GetDatum(-1));
				sign = value < 0 ? false : true;
				asString = std::to_string(value);
			}
			else if (colType->getPrecision() <= 38)
			{
				orc::Decimal128VectorBatch *d128vec = dynamic_cast<orc::Decimal128VectorBatch *>(batch);
				scale = d128vec->scale;
				orc::Int128 *values = d128vec->values.data();
				orc::Int128 value = values[rowIndex];
				if (value == 0)
					return DirectFunctionCall3(numeric_in, CStringGetDatum("0"), ObjectIdGetDatum(0), Int32GetDatum(-1));
				sign = value < 0 ? false : true;
				asString = value.toString();
			}
			else
			{
				elog(ERROR, "ORC Decimal precision more than 38 is not support");
			}

			// Read the interval string
			Assert(scale >= 0);
			if (scale == 0)
				return DirectFunctionCall3(numeric_in, CStringGetDatum(asString.data()), ObjectIdGetDatum(0), Int32GetDatum(-1));

			char numStr[64] = {0};
			char *start = numStr;
			const char *origin = asString.c_str();
			int precision = static_cast<int>(asString.length());

			if (sign == false)
			{
				*start = '-';
				start = numStr + 1;
				++origin;
				--precision;
			}

			if (scale >= precision)
			{
				memset(start, '0', scale + 2);
				start[1] = '.';
				memcpy(start + 2 + scale - precision, origin, precision);
			}
			else
			{
				int intlen = precision - scale;
				memcpy(start, origin, intlen);
				start[intlen] = '.';
				memcpy(start + intlen + 1, origin + intlen, scale);
			}
			return DirectFunctionCall3(numeric_in, CStringGetDatum(numStr), ObjectIdGetDatum(0), Int32GetDatum(-1));
		}
		case CHAROID:
		{
			orc::StringVectorBatch *vec = dynamic_cast<orc::StringVectorBatch *>(batch);
			return CharGetDatum(vec->data[rowIndex][0]);
		}
		case BYTEAOID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			orc::StringVectorBatch *vec = dynamic_cast<orc::StringVectorBatch *>(batch);
			int64_t size = vec->length[rowIndex];
			if (size > BUFFER_LENGTH)
			{
				bytea *result = (bytea*)palloc(size + VARHDRSZ);
				memcpy(VARDATA(result), vec->data[rowIndex], size);
				SET_VARSIZE(result, size + VARHDRSZ);
				return PointerGetDatum(result);
			}
			else
			{
				dataBuffer *ptr = options.buffer.getDataBuffer(columnIndex);
				SET_VARSIZE(ptr->buffer, size + VARHDRSZ);
				memcpy(VARDATA(ptr->buffer), vec->data[rowIndex], size);
				return PointerGetDatum(ptr->buffer);
			}
		}
		default:
			return 0;
    }

    return 0;
}

bool orcFileReader::compareToDeleteMap(orcReadDeltaFile &compact, int index)
{
	orc::StructVectorBatch *batchVector = dynamic_cast<orc::StructVectorBatch *>(readInterface.batch.get());
    if (compact.compareMapIsEmpty())
    {
        return false;
    }
    transcationStruct data;
    orc::ColumnVectorBatch *batch = batchVector->fields[0];
    orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.operation = vec->data[index];

    batch = batchVector->fields[1];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.originalTransaction = vec->data[index];

    batch = batchVector->fields[2];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.bucketId = vec->data[index];

    batch = batchVector->fields[3];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.rowId = vec->data[index];

    batch = batchVector->fields[4];
    vec = dynamic_cast<orc::LongVectorBatch *>(batch);
    data.currentTransaction = vec->data[index];

    if (compact.compareTo(data) == ROW_EQUAL_OTHER)
    {
        return true;
    }
    return false;
}

}
}
