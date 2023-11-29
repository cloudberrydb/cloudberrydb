#include <parquet/arrow/util/decimal.h>
#include <parquet/schema.h>
#include <parquet/column_writer.h>
#include <parquet/arrow/util/bit_util.h>

#include "parquetFileWriter.h"

extern "C"
{
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

#define BATCH_WRITE_SIZE (1024)
#define DECIMAL_FIXBUFFER_SIZE (16)
#define GP_NUMERIC_MAX_SIZE (1000)
#define PARQUET_ROW_GROUP_MAX_SIZE (64*1024*1024)

bool parquetFileWriter::createParquetWriter(ossFileStream ossFile, std::string fileName, void *sstate, writeOption option)
{
	dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;

    Relation relation = ss->rel;
    if (relation == NULL) {
        elog(ERROR, "Parquet get Relation failed\n");
    }
    estimated_bytes = 0;
    ncolumns = relation->rd_att->natts;
    tupdesc = relation->rd_att;
    this->option = option;
    name = fileName;
    createColumnBatch();
    schema = setupSchema();
    writeProperties();

    out_file = std::make_shared<gopherWriteFileSystem>(ossFile);
    out_file->OpenFile(name.c_str());
    file_writer = parquet::ParquetFileWriter::Open(out_file, schema, props);
    batchNum = 0;
    dataBufferOffset = 0;
    dataBuffer.resize(1024*1024);
    rg_writer = file_writer->AppendBufferedRowGroup();
    return true;
}

void parquetFileWriter::createColumnBatch()
{
    byteArray = (parquet::ByteArray*)palloc(sizeof(parquet::ByteArray) * BATCH_WRITE_SIZE);
    fixByteArray = (parquet::FixedLenByteArray*)palloc(sizeof(parquet::FixedLenByteArray) * BATCH_WRITE_SIZE);
    definition_level = (int16_t*)palloc(sizeof(int16_t) * BATCH_WRITE_SIZE);
    int96Array = (parquet::Int96*)palloc(sizeof(parquet::Int96) * BATCH_WRITE_SIZE);
    decimalOutBuf = (uint8_t*)palloc(DECIMAL_FIXBUFFER_SIZE * BATCH_WRITE_SIZE);

    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeId = tupdesc->attrs[i].atttypid;
        std::string columnName = tupdesc->attrs[i].attname.data;
        switch (typeId)
        {
            case BOOLOID: {
                columnBatch<bool> *val = (columnBatch<bool>*)palloc(sizeof(columnBatch<bool>));
                val->buffer = (bool*)palloc(sizeof(bool) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;

            }
            case INT2OID:
            case INT4OID: {
                columnBatch<int32_t> *val = (columnBatch<int32_t>*)palloc(sizeof(columnBatch<int32_t>));
                val->buffer = (int32_t*)palloc(sizeof(int32_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case INT8OID: {
                columnBatch<int64_t> *val = (columnBatch<int64_t>*)palloc(sizeof(columnBatch<int64_t>));
                val->buffer = (int64_t*)palloc(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case FLOAT4OID: {
                columnBatch<float> *val = (columnBatch<float>*)palloc(sizeof(columnBatch<float>));
                val->buffer = (float*)palloc(sizeof(float) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case FLOAT8OID: {
                columnBatch<double> *val = (columnBatch<double>*)palloc(sizeof(columnBatch<double>));
                val->buffer = (double*)palloc(sizeof(double) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case DATEOID: {
                columnBatch<int32_t> *val = (columnBatch<int32_t>*)palloc(sizeof(columnBatch<int32_t>));
                val->buffer = (int32_t*)palloc(sizeof(int32_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
                columnBatch<int64_t> *val = (columnBatch<int64_t>*)palloc(sizeof(columnBatch<int64_t>));
                val->buffer = (int64_t*)palloc(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case NUMERICOID: {
                StringVectorBatch *val = (StringVectorBatch*)palloc(sizeof(StringVectorBatch));
                val->buffer = (char**)palloc(sizeof(char*) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->precision = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->scale = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case BYTEAOID:
            case TEXTOID:
            case INTERVALOID:
            case TIMEOID: {
                StringVectorBatch *val = (StringVectorBatch*)palloc(sizeof(StringVectorBatch));
                val->buffer = (char**)palloc(sizeof(char*) * BATCH_WRITE_SIZE);
                val->length = (int64_t*)palloc0(sizeof(int64_t) * BATCH_WRITE_SIZE);
                val->notNull = (bool*)palloc0(sizeof(bool) * BATCH_WRITE_SIZE);
                batchField.push_back(val);
                break;
            }
            default:
                elog(ERROR, "External Table Type Mismatch: MPP type %s not define in parquet type %s. type mapping %s",
                    tupdesc->attrs[i].attname.data,
                    getColTypeName(typeId).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
}

std::shared_ptr<parquet::schema::GroupNode> parquetFileWriter::setupSchema()
{
    parquet::schema::NodeVector fields;
    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeId = tupdesc->attrs[i].atttypid;
        std::string columnName = tupdesc->attrs[i].attname.data;
        switch (typeId)
        {
            case BOOLOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BOOLEAN));
                break;
            }
            case INT2OID:
            case INT4OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32));
                break;
            }
            case INT8OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT64));
                break;
            }
            case FLOAT4OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::FLOAT));
                break;
            }
            case FLOAT8OID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::DOUBLE));
                break;
            }
            case DATEOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT32, ::parquet::ConvertedType::DATE));
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT96));
                break;
            }
            case NUMERICOID: {
                int64_t precision = 0;
                int64_t scale = 0;
                if (tupdesc->attrs[i].atttypmod < 0)
                {
                    precision = 38;
                    scale = GP_NUMERIC_MAX_SIZE;
                }
                else
                {
                    precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
                    scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
                }
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(), ::parquet::Repetition::OPTIONAL,
                    ::parquet::Type::FIXED_LEN_BYTE_ARRAY, ::parquet::ConvertedType::DECIMAL, DECIMAL_FIXBUFFER_SIZE, precision, scale));
                break;
            }
            case CHAROID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            case BPCHAROID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            case VARCHAROID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            case BYTEAOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            case TEXTOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            case INTERVALOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            case TIMEOID: {
                fields.push_back(::parquet::schema::PrimitiveNode::Make(columnName.c_str(),
                    ::parquet::Repetition::OPTIONAL, ::parquet::Type::BYTE_ARRAY));
                break;
            }
            default:
                elog(ERROR, "External Table Type Mismatch: MPP type %s not define in parquet type %s. type mapping %s",
                    tupdesc->attrs[i].attname.data,
                    getColTypeName(typeId).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
        ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::OPTIONAL, fields));
}

void parquetFileWriter::writeProperties()
{
    parquet::WriterProperties::Builder builder;
    parquet_arrow::Compression::type codec_type = parquet_arrow::Compression::UNCOMPRESSED;
    switch (option.compression) {
        case UNCOMPRESS:{
            codec_type = parquet_arrow::Compression::UNCOMPRESSED;
            break;
        }
        case SNAPPY: {
            codec_type = parquet_arrow::Compression::SNAPPY;
            break;
        }
        case GZIP: {
            codec_type = parquet_arrow::Compression::GZIP;
            break;
        }
        case BROTLI: {
            codec_type = parquet_arrow::Compression::BROTLI;
            break;
        }
        case ZSTD: {
            codec_type = parquet_arrow::Compression::ZSTD;
            break;
        }
        case LZ4: {
            codec_type = parquet_arrow::Compression::LZ4;
            break;
        }
        default:
            codec_type = parquet_arrow::Compression::UNCOMPRESSED;
            break;
    }

    builder.compression(codec_type);
    builder.created_by("Hashdata");
    builder.max_row_group_length(PARQUET_ROW_GROUP_MAX_SIZE);
    builder.data_pagesize(1024*1024);
    builder.disable_dictionary();
    props = builder.build();
}

void parquetFileWriter::resetParquetWriter()
{
    if (batchNum > 0)
    {
        writeToBatch(batchNum);
        batchNum = 0;
        estimated_bytes = 0;
    }
    rg_writer->Close();
    file_writer->Close();
    out_file->Close();
    schema.reset();
    props.reset();
    file_writer.reset();
    out_file.reset();
    dataBufferOffset = 0;
    if (byteArray != NULL)
    {
        pfree(byteArray);
        byteArray = NULL;
    }
    if (fixByteArray != NULL)
    {
        pfree(fixByteArray);
        fixByteArray = NULL;
    }
    if (definition_level != NULL)
    {
        pfree(definition_level);
        definition_level = NULL;
    }
    if (int96Array != NULL)
    {
        pfree(int96Array);
        int96Array = NULL;
    }
    if (decimalOutBuf != NULL)
    {
        pfree(decimalOutBuf);
        decimalOutBuf = NULL;
    }
}

void parquetFileWriter::closeParquetWriter()
{
    resetParquetWriter();
}


void parquetFileWriter::resizeDataBuff(int count, std::vector<char>& buffer, int64_t datalen, int offset)
{
    char* oldBufferAddress = buffer.data();
    bool isResize = false;
    int64_t oldSize = buffer.size();
    int64_t newSize = oldSize;
    while (newSize - offset < datalen) {
        newSize = newSize * 2;
        isResize = true;
    }
    if (isResize)
    {
        buffer.resize(newSize);
        char* newBufferAddress = buffer.data();
        reDistributedDataBuffer(count, oldBufferAddress, newBufferAddress);
    }
}

bool parquetFileWriter::columnBelongStringType(int attColumn)
{
    switch (attColumn)
    {
        case CHAROID:
        case BPCHAROID:
        case VARCHAROID:
        case BYTEAOID:
        case TEXTOID:
        case INTERVALOID:
        case TIMEOID:
        case NUMERICOID:
        {
            return true;
        }
        default:
            break;
    }
    return false;
}

void parquetFileWriter::reDistributedDataBuffer(int count, char* oldBufferAddress, char* newBufferAddress)
{
    for (int row = 0; row < count + 1; row++)
    {
        for (int i = 0; i < ncolumns; i++)
        {
            if (columnBelongStringType(tupdesc->attrs[i].atttypid))
            {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                bool notNull = val->notNull[row];
                if (notNull)
                {
                    val->buffer[row] = val->buffer[row] - oldBufferAddress + newBufferAddress;
                }
            }
        }
    }
}

int64_t parquetFileWriter::write(const void* buf, size_t length)
{
    writeToField(batchNum, buf);
    batchNum++;
    if (batchNum >= BATCH_WRITE_SIZE)
    {
        writeToBatch(batchNum);
        batchNum = 0;
        dataBufferOffset = 0;
        estimated_bytes = 0;
    }
    return length;
}

void parquetFileWriter::writeToField(int index, const void* data)
{
	TupleTableSlot* slot = (TupleTableSlot*)data;
    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeID = tupdesc->attrs[i].atttypid;
		Datum tts_values = slot->tts_values[i];
		bool isNULL = slot->tts_isnull[i];

        switch (tupdesc->attrs[i].atttypid) {
            case BOOLOID: {
                columnBatch<bool> * val = reinterpret_cast<columnBatch<bool>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetBool(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(bool);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case INT2OID:
            case INT4OID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetInt32(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int32_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case INT8OID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetInt64(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int64_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case FLOAT4OID: {
                columnBatch<float> * val = reinterpret_cast<columnBatch<float>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetFloat4(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(float);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case FLOAT8OID: {
                columnBatch<double> * val = reinterpret_cast<columnBatch<double>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetFloat8(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(double);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case DATEOID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetDateADT(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int32_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case TIMESTAMPOID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                if (!isNULL)
                {
                    val->buffer[index] = DatumGetTimestamp(tts_values);
                    val->notNull[index] = true;
                    val->num = index;
                    estimated_bytes += sizeof(int64_t);
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case NUMERICOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    int64_t precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
                    int64_t scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
                    char *data = DatumGetCString(DirectFunctionCall1(numeric_out, tts_values));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);
                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->precision[index] = precision;
                    val->scale[index] = scale;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case BYTEAOID:
            case TEXTOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(textout, tts_values));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case INTERVALOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(interval_out, tts_values));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            case TIMEOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                if (!isNULL)
                {
                    char *data = DatumGetCString(DirectFunctionCall1(time_out, DatumGetTimeADT(tts_values)));
                    int64_t datalen = static_cast<int64_t> (strlen(data));
                    resizeDataBuff(index, dataBuffer, datalen, dataBufferOffset);
                    memcpy(dataBuffer.data() + dataBufferOffset, data, datalen);

                    val->buffer[index] = dataBuffer.data() + dataBufferOffset;
                    val->length[index] = datalen;
                    val->notNull[index] = true;
                    val->num = index;
                    dataBufferOffset += datalen;
                    if (data != NULL)
                    {
                        pfree(data);
                    }
                    estimated_bytes += datalen;
                }
                else
                {
                    val->notNull[index] = false;
                }
                break;
            }
            default:
                std::string columnName = tupdesc->attrs[i].attname.data;
                elog(ERROR,
                    "Type Mismatch: data in %s is as define %s in external table, but in parquet not define %s",
                    columnName.c_str(),
                    getColTypeName(typeID).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
}

void parquetFileWriter::writeToBatch(int rows)
{
    if (rg_writer->total_bytes_written() + rg_writer->total_compressed_bytes() + estimated_bytes >= PARQUET_ROW_GROUP_MAX_SIZE)
    {
        rg_writer->Close();
        rg_writer = file_writer->AppendBufferedRowGroup();
    }
    for (int i = 0; i < ncolumns; i++)
    {
        Oid typeID = tupdesc->attrs[i].atttypid;
        switch (tupdesc->attrs[i].atttypid) {
            case BOOLOID: {
                columnBatch<bool> * val = reinterpret_cast<columnBatch<bool>*>(batchField[i]);
                parquet::BoolWriter* writer = static_cast<parquet::BoolWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case INT2OID:
            case INT4OID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case INT8OID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                parquet::Int64Writer* writer = static_cast<parquet::Int64Writer*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case FLOAT4OID: {
                columnBatch<float> * val = reinterpret_cast<columnBatch<float>*>(batchField[i]);
                parquet::FloatWriter* writer = static_cast<parquet::FloatWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case FLOAT8OID: {
                columnBatch<double> * val = reinterpret_cast<columnBatch<double>*>(batchField[i]);
                parquet::DoubleWriter* writer = static_cast<parquet::DoubleWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case DATEOID: {
                columnBatch<int32_t> * val = reinterpret_cast<columnBatch<int32_t>*>(batchField[i]);
                parquet::Int32Writer* writer = static_cast<parquet::Int32Writer*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        int32_t value = val->buffer[row] + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
                        val->buffer[row] = value;
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, val->buffer);
                break;
            }
            case TIMESTAMPOID: {
                columnBatch<int64_t> * val = reinterpret_cast<columnBatch<int64_t>*>(batchField[i]);
                parquet::Int96Writer* writer = static_cast<parquet::Int96Writer*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        int64_t timestamp = val->buffer[row];
                        struct pg_tm tt, *tm = &tt;
                        fsec_t fsec;
                        timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL);
                        int64_t second = (tm->tm_hour * 60 + tm->tm_min) * 60 + tm->tm_sec;
                        int64_t nanoSecond = (second * 1000000 + fsec) * 1000;
                        int32_t day = date2j(tm->tm_year, tm->tm_mon, tm->tm_mday);
                        int96Array[row].value[2] = day;
                        parquet::Int96SetNanoSeconds(int96Array[row], nanoSecond);
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, int96Array);
                break;
            }
            case NUMERICOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::FixedLenByteArrayWriter* writer = static_cast<parquet::FixedLenByteArrayWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                memset(decimalOutBuf, 0, DECIMAL_FIXBUFFER_SIZE * BATCH_WRITE_SIZE);
                decimalOutBufOffset = 0;
                char buffer[256] = {0};
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        int64_t datalen = val->length[row];
                        memcpy(buffer, val->buffer[row], datalen);
                        buffer[datalen] = '\0';
                        std::string decimal = buffer;
                        parquet_arrow::Decimal128 ptr = parquet_arrow::Decimal128(decimal.c_str());
                        uint8_t* out_buf = decimalOutBuf + decimalOutBufOffset;
                        ptr.ToBytes(out_buf);
                        int64_t typeLen = DECIMAL_FIXBUFFER_SIZE;
                        parquet_arrow::Result<parquet_arrow::Decimal128> t = parquet_arrow::Decimal128::FromBigEndian(out_buf, typeLen);
                        auto v = t.MoveValueUnsafe();
                        v.ToBytes(out_buf);
                        fixByteArray[row].ptr = reinterpret_cast<const uint8_t*>(out_buf);
                        definition_level[row] = 1;
                        decimalOutBufOffset += DECIMAL_FIXBUFFER_SIZE;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, fixByteArray);
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case BYTEAOID:
            case TEXTOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        byteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        byteArray[row].len = val->length[row];
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, byteArray);
                break;
            }
            case INTERVALOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        byteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        byteArray[row].len = val->length[row];
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, byteArray);
                break;
            }
            case TIMEOID: {
                StringVectorBatch* val = reinterpret_cast<StringVectorBatch*>(batchField[i]);
                parquet::ByteArrayWriter* writer = static_cast<parquet::ByteArrayWriter*>(rg_writer->column(i));
                std::vector<uint8_t> valid_bits(parquet_arrow::BitUtil::BytesForBits(BATCH_WRITE_SIZE), 255);
                for (int row = 0; row < rows; row++)
                {
                    bool notNull = val->notNull[row];
                    if (notNull)
                    {
                        byteArray[row].ptr = reinterpret_cast<const uint8_t*>(val->buffer[row]);
                        byteArray[row].len = val->length[row];
                        definition_level[row] = 1;
                    }
                    else
                    {
                        definition_level[row] = 0;
                        parquet_arrow::BitUtil::ClearBit(valid_bits.data(), row);
                    }
                }
                writer->WriteBatchSpaced(rows, definition_level, nullptr, valid_bits.data(), 0, byteArray);
                break;
            }
            default:
                std::string columnName = tupdesc->attrs[i].attname.data;
                elog(ERROR,
                    "Type Mismatch: data in %s is as define %s in external table, but in parquet not define %s",
                    columnName.c_str(),
                    getColTypeName(typeID).data(),
                    getTypeMappingSupported().data());
                break;
        }
    }
}
