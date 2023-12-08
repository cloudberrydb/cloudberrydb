#include <parquet/arrow/util/decimal.h>
#include <parquet/arrow/result.h>
#include "parquetFileReader.h"
#include "parquetInputStream.h"


extern "C"
{
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

namespace Datalake {
namespace Internal {

parquetFileReader::parquetFileReader()
{
    row_count = 0;
    row_offset = 0;
    name = "";
    num_row_groups = 0;
    num_columns = 0;
    parquet_reader.reset();
    row_group_reader.reset();
    scanners.clear();
    state = FILE_UNDEF;
}

// void parquetFileReader::keepScannerForPartitonTable()
// {
// 	/* Keep a scanner get data. If foreign table only select partition key.
// 	 * example:
// 	 * 1. create foreign table test(a text, b text, c text);
// 	 *    'c' is hive partition key.
// 	 * 2. If only select * from test where c='xx';
// 	 * 3. parquet will be a dead cycle. so we keep a scanner in scanners If
// 	 *    partition table.
// 	 */

// 	if (options.nPartitionKey <= 0)
// 	{
// 		/* not partiton table */
// 		return;
// 	}
// 	bool table_columns_all_null = true;
// 	int ncolumns = options.includes_columns.size() - options.nPartitionKey;
//     for (int i = 0; i < ncolumns; i++)
//     {
// 		if (options.includes_columns[i])
// 		{
// 			table_columns_all_null = false;
// 			break;
// 		}
// 	}

// 	if (table_columns_all_null)
// 	{

// 	}
// }

bool parquetFileReader::createParquetReader(ossFileStream ossFile, std::string fileName, readOption options)
{

    this->options = options;
    if (!createInternalReader(ossFile, fileName))
    {
        return false;
    }
    int ncolumns = options.includes_columns.size() - options.nPartitionKey;
    for (int i = 0; i < ncolumns; i++)
    {
        scanners.push_back(NULL);
    }
    return true;
}

void parquetFileReader::resetParquetReader()
{
    row_count = 0;
    row_offset = 0;
    if (state != FILE_UNDEF)
    {
        parquet_reader->Close();
		fileStream->Close();
		fileStream.reset();
    }
    resetScanners();
    file_metadata.reset();
    parquet_reader.reset();
    num_row_groups = 0;
    num_columns = 0;
}

bool parquetFileReader::createInternalReader(ossFileStream ossFile, std::string fileName)
{
	fileStream = std::make_shared<gopherReadFileSystem>(ossFile, fileName, setStreamWhetherCache(options));
    fileStream->Open();
    if (!fileStream->checkFileIsParquet())
    {
        return false;
    }
    parquet_reader = parquet::ParquetFileReader::Open(fileStream);
    file_metadata = parquet_reader->metadata();
    num_row_groups = file_metadata->num_row_groups();
    num_columns = file_metadata->num_columns();
    name = fileName;
    int mpp_columns = options.includes_columns.size() - options.nPartitionKey;
    if (mpp_columns != num_columns)
    {
        elog(ERROR, "External table Error, mpp columns %d not equal parquet %s columns %d.",
            mpp_columns, fileName.c_str(), num_columns);
    }
    state = FILE_OPEN;
    return true;
}

void parquetFileReader::setRowGroup(int row_group_num)
{
    row_offset = 0;
    row_group_reader = parquet_reader->RowGroup(row_group_num);
    row_count = row_group_reader->metadata()->num_rows();
}

int64_t parquetFileReader::rowGroupOffset(int row_group_num)
{
    int64_t offset = parquet_reader->RowGroup(row_group_num)->metadata()->file_offset();

    return offset;
}

void parquetFileReader::createScanners()
{
    int index = 0;
    for (int i = 0; i < num_columns; i++)
    {
        if (options.nPartitionKey <= 0 && !options.includes_columns[i])
        {
            // not select columns continue
            index++;
            continue;
        }
        std::shared_ptr<parquet::ColumnReader> col_reader = row_group_reader->Column(i);
        scanners[index] = ::parquet::Scanner::Make(col_reader, options.batch_size);
        index++;
    }
}

void parquetFileReader::resetScanners() {
    scanners.clear();
}


Datum parquetFileReader::read(Oid typeOid, int column_index, bool &isNull, int &state)
{
    const parquet::ColumnDescriptor* des = file_metadata->schema()->Column(column_index);
    if (!checkDataTypeCompatible(typeOid, des->physical_type()))
    {
        elog(ERROR, "Type Mismatch: columnIndex %d. MPP type %s. Parquet type %s. External Type Mapping %s",
            column_index, getColTypeName(typeOid).c_str(), des->name().c_str(), getTypeMappingSupported().c_str());
    }

    auto scanner = scanners[column_index];
    switch(typeOid)
    {
        case BOOLOID:
        {
            parquet::TypedScanner<parquet::BooleanType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::BooleanType>*>(scanner.get());
            bool values = false;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return BoolGetDatum(values);
        }
        case INT8OID:
        {
            parquet::TypedScanner<parquet::Int64Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int64Type>*>(scanner.get());
            int64_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int64GetDatum(values);
        }
        case INT4OID:
        {
            parquet::TypedScanner<parquet::Int32Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int32Type>*>(scanner.get());
            int32_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int32GetDatum(values);
        }
        case INT2OID:
        {
            parquet::TypedScanner<parquet::Int32Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int32Type>*>(scanner.get());
            int32_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int16GetDatum(values);
        }
        case FLOAT4OID:
        {
            parquet::TypedScanner<parquet::FloatType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::FloatType>*>(scanner.get());
            float values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Float4GetDatum(values);
        }
        case FLOAT8OID:
        {
            parquet::TypedScanner<parquet::DoubleType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::DoubleType>*>(scanner.get());
            double values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Float8GetDatum(values);
        }
        case INTERVALOID:
        {
            parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::ByteArrayType>*>(scanner.get());
            parquet::ByteArray values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            char readBuf[1024] = {0};
            memcpy(readBuf, values.ptr, values.len);
            return DirectFunctionCall1(interval_in, CStringGetDatum(readBuf));
        }
        case DATEOID:
        {
            parquet::TypedScanner<parquet::Int32Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int32Type>*>(scanner.get());
            int32_t values = 0;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            return Int32GetDatum(values  + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
        }
        case TIMEOID:
        {
            parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::ByteArrayType>*>(scanner.get());
            parquet::ByteArray values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            char readBuf[1024] = {0};
            memcpy(readBuf, values.ptr, values.len);
            return DirectFunctionCall1(time_in, CStringGetDatum(readBuf));
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        {
            parquet::TypedScanner<parquet::Int96Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int96Type>*>(scanner.get());
            parquet::Int96 values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            int64_t second = parquet::Int96GetSeconds(values);
            int64_t micsecs = (second + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) *60*60*24) * 1000000;
            int64_t diff_micsecs = parquet::Int96GetMicroSeconds(values) - second * 1000000;
            int64_t timestamp = micsecs + diff_micsecs;
            return Int64GetDatum(timestamp);
        }
        case NUMERICOID:
        {
            switch(des->physical_type())
            {
                case ::parquet::Type::INT32: {
                    parquet::TypedScanner<parquet::Int32Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int32Type>*>(scanner.get());
                    int32_t values = 0;
                    uint scale = des->type_scale();
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    std::string num_str = std::to_string(values);
                    if (scale >= num_str.size())
                    {
                        std::string prefix = "0.";
                        prefix.append(scale-num_str.size(), '0');
                        num_str.insert(0, prefix);
                    }
                    else
                    {
                        num_str.insert(num_str.size() - scale, ".");
                    }
                    return DirectFunctionCall3(numeric_in, CStringGetDatum(num_str.c_str()), ObjectIdGetDatum(0), Int32GetDatum(-1));

                }
                case ::parquet::Type::INT64: {
                    parquet::TypedScanner<parquet::Int64Type>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::Int64Type>*>(scanner.get());
                    int64_t values = 0;
                    uint scale = des->type_scale();
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    std::string num_str = std::to_string(values);
                    if (scale >= num_str.size())
                    {
                        std::string prefix = "0.";
                        prefix.append(scale-num_str.size(), '0');
                        num_str.insert(0, prefix);
                    }
                    else
                    {
                        num_str.insert(num_str.size() - scale, ".");
                    }
                    return DirectFunctionCall3(numeric_in, CStringGetDatum(num_str.c_str()), ObjectIdGetDatum(0), Int32GetDatum(-1));
                }
                case ::parquet::Type::BYTE_ARRAY: {
                    int precision = des->type_precision();
                    int scale = des->type_scale();
                    if (precision > 0)
                    {
                        //TODO
                    }
                    if (scale > 0)
                    {
                        //TODO
                    }
                    parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::ByteArrayType>*>(scanner.get());
                    parquet::ByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    uint8_t out_buf[256] = {0};
                    parquet_arrow::Result<parquet_arrow::Decimal256> t = parquet_arrow::Decimal256::FromBigEndian(values.ptr, values.len);
                    auto v = t.MoveValueUnsafe();
                    v.ToBytes(out_buf);
                    const parquet_arrow::Decimal256 value(out_buf);
                    std::string decimal = value.ToString(scale);
                    if (decimal == "<scale out of range, cannot format Decimal128 value>")
                    {
                        elog(ERROR, "External table Error, parquet error <scale out of range, cannot format Decimal128 value>");
                    }
                    return DirectFunctionCall3(numeric_in, CStringGetDatum(decimal.c_str()), ObjectIdGetDatum(0), Int32GetDatum(-1));
                }
                case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                    int precision = des->type_precision();
                    int scale = des->type_scale();
                    if (precision > 38)
                    {
                        elog(ERROR, "Parquet Decimal128 precision more than 38 is not support");
                    }
                    if (scale > 0)
                    {
                        //TODO
                    }
                    int type_len = des->type_length();
                    parquet::TypedScanner<parquet::FLBAType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::FLBAType>*>(scanner.get());
                    parquet::FixedLenByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    uint8_t out_buf[128] = {0};
                    parquet_arrow::Result<parquet_arrow::Decimal128> t = parquet_arrow::Decimal128::FromBigEndian(values.ptr, type_len);
                    auto v = t.MoveValueUnsafe();
                    v.ToBytes(out_buf);
                    const parquet_arrow::Decimal128 value(out_buf);
                    std::string decimal = value.ToString(scale);
                    if (decimal == "<scale out of range, cannot format Decimal128 value>")
                    {
                        elog(ERROR, "External table Error, parquet error <scale out of range, cannot format Decimal128 value>");
                    }
                    return DirectFunctionCall3(numeric_in, CStringGetDatum(decimal.c_str()), ObjectIdGetDatum(0), Int32GetDatum(-1));
                }
                default:
                    elog(ERROR, "External table Error, Column index %d Parquet format "
                    "Physical type for decimal128 must be int32, int64, byte array, "
                    "or fixed length binary. physical_type %d.", typeOid, des->physical_type());
            }
        }
        case CHAROID:
        {
            parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::ByteArrayType>*>(scanner.get());
            parquet::ByteArray values;
            bool hasRow = scanner_->NextValue(&values, &isNull);
            if (!hasRow)
            {
                //no row to read
                state = -1;
                return 0;
            }
            if (isNull)
            {
                return 0;
            }
            char result[5] = {0};
            memcpy(result, values.ptr, values.len);
            return CharGetDatum(result[0]);
        }
        case BYTEAOID:
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
        {
            switch(des->physical_type())
            {
                case ::parquet::Type::BYTE_ARRAY: {
                    parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::ByteArrayType>*>(scanner.get());
                    parquet::ByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    if (values.len > BUFFER_LENGTH)
                    {
                        bytea *result = (bytea*)palloc(values.len + VARHDRSZ);
                        memcpy(VARDATA(result), values.ptr, values.len);
                        SET_VARSIZE(result, values.len + VARHDRSZ);
                        return PointerGetDatum(result);
                    }
                    else
                    {
                        dataBuffer* ptr = options.buffer.getDataBuffer(column_index);
                        memcpy(VARDATA(ptr->buffer), values.ptr, values.len);
                        SET_VARSIZE(ptr->buffer, values.len + VARHDRSZ);
                        return PointerGetDatum(ptr->buffer);
                    }
                }
                case ::parquet::Type::FIXED_LEN_BYTE_ARRAY: {
                    int type_len = des->type_length();
                    parquet::TypedScanner<parquet::ByteArrayType>* scanner_ = reinterpret_cast<parquet::TypedScanner<parquet::ByteArrayType>*>(scanner.get());
                    parquet::ByteArray values;
                    bool hasRow = scanner_->NextValue(&values, &isNull);
                    if (!hasRow)
                    {
                        //no row to read
                        state = -1;
                        return 0;
                    }
                    if (isNull)
                    {
                        return 0;
                    }
                    if (values.len > BUFFER_LENGTH)
                    {
                        bytea *result = (bytea*)palloc(values.len + VARHDRSZ);
                        memcpy(VARDATA(result), values.ptr, type_len);
                        SET_VARSIZE(result, values.len + VARHDRSZ);
                        return PointerGetDatum(result);
                    }
                    else
                    {
                        dataBuffer* ptr = options.buffer.getDataBuffer(column_index);
                        memcpy(VARDATA(ptr->buffer), values.ptr, values.len);
                        SET_VARSIZE(ptr->buffer, values.len + VARHDRSZ);
                        return PointerGetDatum(ptr->buffer);
                    }
                }
                default:
                    elog(ERROR, "External table Error, Column index %d. "
                     "MPP type BYTEAOID/TEXTOID/BPCHAROID/VARCHAROID Mapping "
                     "parquet BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY. but column %d.", 
                     typeOid, des->physical_type());
            }
        }
        default:
            return 0;

    }
    return 0;
}

void parquetFileReader::closeParquetReader()
{
    if (state == FILE_CLOSE)
    {
        return;
    }
    resetParquetReader();
    state = FILE_CLOSE;
}

}
}