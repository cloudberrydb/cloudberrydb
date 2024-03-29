#include <parquet/arrow/util/decimal.h>
#include <sstream>

#include "avroOssOutputStream.h"
#include "avroWriter.h"

#include "src/provider/provider.h"
extern "C"
{
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

#define AVRO_SCHEMA_NAME "CBDB_DATALAKE"
#define AVRO_IS_NULL (0)
#define AVRO_NOT_NULL (1)
#define AVRO_WRITE_BUFFER_SIZE (8 * 1024 * 1024)

template <typename T>
static T stringToNum(const std::string& str)
{
    std::istringstream iss(str);
    T num;
    iss >> num;
    return num;
}

avroWriter::avroWriter(ossFileStream ossFile, const std::string& fileName, void* sstate, writeOption option)
{
    dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;

    Relation relation = ss->rel;
    if (relation == NULL)
    {
        elog(ERROR, "Avro get Relation failed\n");
    }
    ncolumns = relation->rd_att->natts;
    tupdesc = relation->rd_att;

    initCompression(option);
    initSchema();
    bufDatum = std::make_unique<avro::GenericDatum>(*schema);
    avroOssOutputStreamPtr out_file = std::make_unique<avroOssOutputStream>(ossFile, fileName, AVRO_WRITE_BUFFER_SIZE);
    file_writer = std::make_unique<avro::DataFileWriter<avro::GenericDatum>>(std::move(out_file), *schema, AVRO_WRITE_BUFFER_SIZE, compression);
}

avroWriter::~avroWriter()
{
}

int64_t avroWriter::write(const void* data, size_t length)
{
    TupleTableSlot *slot = (TupleTableSlot*)data;
    avro::GenericRecord &buf = bufDatum->value<avro::GenericRecord>();
    for (int i = 0; i < ncolumns; ++i)
    {
        Oid typeID = tupdesc->attrs[i].atttypid;
        Datum tts_values = slot->tts_values[i];
        bool isNull = slot->tts_isnull[i];
        if (isNull)
        {
            buf.fieldAt(i).selectBranch(AVRO_IS_NULL);
            continue;
        }
        buf.fieldAt(i).selectBranch(AVRO_NOT_NULL);
        switch (typeID) {
            case BOOLOID:
            {
                buf.fieldAt(i).value<bool>() = DatumGetBool(tts_values);
                break;
            }
            case INT2OID:
            case INT4OID:
            {
                buf.fieldAt(i).value<int32_t>() = DatumGetInt32(tts_values);
                break;
            }
            case INT8OID:
            {
                buf.fieldAt(i).value<int64_t>() = DatumGetInt64(tts_values);
                break;
            }
            case FLOAT4OID:
            {
                buf.fieldAt(i).value<float>() = DatumGetFloat4(tts_values);
                break;
            }
            case FLOAT8OID:
            {
                buf.fieldAt(i).value<double>() = DatumGetFloat8(tts_values);
                break;
            }
            case NUMERICOID:
            {
                char *data = DatumGetCString(DirectFunctionCall1(numeric_out, tts_values));
                int precision = buf.fieldAt(i).logicalType().precision();
                std::vector<uint8_t> &vecBuf = buf.fieldAt(i).value<std::vector<uint8_t>>();
                vecBuf.clear();
                if (precision <= 9)
                {
                    std::string decimalStr(data);
                    decimalStr.erase(decimalStr.find('.'), 1);
                    uint32_t decimal = stringToNum<int32_t>(decimalStr);
                    for (int i = 24; i >= 0; i -= 8)
                    {
                        vecBuf.push_back((decimal >> i) & 0xFF);
                    }
                }
                else if (precision <= 18)
                {
                    std::string decimalStr(data);
                    decimalStr.erase(decimalStr.find('.'), 1);
                    int64_t decimal = stringToNum<int64_t>(decimalStr);
                    for (int i = 56; i >= 0; i -= 8)
                    {
                        vecBuf.push_back((decimal >> i) & 0xFF);
                    }
                }
                else if (precision <= AVRO_NUMERIC_128_SIZE)
                {
                    parquet_arrow::Decimal128 decimal(data);
                    const std::array<uint8_t, 16> &bytes = decimal.ToBytes();
                    vecBuf.assign(bytes.begin(), bytes.end());
                    std::reverse(vecBuf.begin(), vecBuf.end());
                }
                else
                {
                    elog(ERROR, "avro decimal is out of range, the precision should less than or equal to %d", AVRO_NUMERIC_MAX_SIZE);
                }
                if (data != NULL)
                {
                    pfree(data);
                }
                break;
            }
            case BYTEAOID:
            {
                char *data = DatumGetCString(DirectFunctionCall1(textout, tts_values));
                int64_t datalen = static_cast<int64_t> (strlen(data));
                std::vector<uint8_t> &bufVec = buf.fieldAt(i).value<std::vector<uint8_t>>();
                std::vector<uint8_t>(datalen).swap(bufVec);
                memcpy(bufVec.data(), data, datalen);
                if (data != NULL)
                {
                    pfree(data);
                }
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case TEXTOID:
            {
                char *text = DatumGetCString(DirectFunctionCall1(textout, tts_values));
                buf.fieldAt(i).value<std::string>() = text;
                if (text != NULL)
                {
                    pfree(text);
                }
                break;
            }
            case DATEOID:
            {
                buf.fieldAt(i).value<int32_t>() = DatumGetDateADT(tts_values) + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
                break;
            }
            case TIMEOID:
            {
                buf.fieldAt(i).value<int64_t>() = DatumGetTimeADT(tts_values);
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            {
                buf.fieldAt(i).value<int64_t>() = DatumGetTimestamp(tts_values) + (int64_t)(POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * 60 * 60 * 24 * 1000000;
                break;
            }
            case UUIDOID:
            {
                buf.fieldAt(i).value<std::string>() = DatumGetCString(DirectFunctionCall1(uuid_out, tts_values));
                break;
            }
        }
    }
    file_writer->write(*bufDatum);
    return length;
}

void avroWriter::close()
{
    file_writer->close();
}

void avroWriter::initSchema()
{
    avro::RecordSchema recordSchema(AVRO_SCHEMA_NAME);
    for (int i = 0; i < ncolumns; ++i)
    {
        Oid typeId = tupdesc->attrs[i].atttypid;
        std::string columnName = tupdesc->attrs[i].attname.data;
        switch (typeId)
        {
            case BOOLOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::BoolSchema()));
                break;
            }
            case INT2OID: 
            case INT4OID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::IntSchema()));
                break;
            }
            case INT8OID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::LongSchema()));
                break;
            }
            case FLOAT4OID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::FloatSchema()));
                break;
            }
            case FLOAT8OID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::DoubleSchema()));
                break;
            }
            case NUMERICOID:
            {
                int precision = 0;
                int scale = 0;
                if (tupdesc->attrs[i].atttypmod < 0)
                {
                    ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("The precision of numeric in foreign tables with avro format should be specified explicitly.")));
                }
                else
                {
                    precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
                    scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
                    if (precision > AVRO_NUMERIC_MAX_SIZE)
                    {
                        elog(ERROR, "The precision of Avro decimal should not be greater than %d", AVRO_NUMERIC_MAX_SIZE);
                    }
                }
                recordSchema.addField(columnName, avro::OptionalSchema(avro::DecimalSchema(precision, scale)));
                break;
            }
            case BYTEAOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::BytesSchema()));
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case TEXTOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::StringSchema()));
                break;
            }
            case DATEOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::DateSchema()));
                break;
            }
            case TIMEOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::TimeSchema()));
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::TimestampSchema()));
                break;
            }
            case UUIDOID:
            {
                recordSchema.addField(columnName, avro::OptionalSchema(avro::UUIDSchema()));
                break;
            }
            default:
            {
                elog(ERROR, "Foreign Table Type Mismatch: MPP type %s not define in avro type %s. type mapping %s",
                    tupdesc->attrs[i].attname.data,
                    getColTypeName(typeId).data(),
                    getTypeMappingSupported().data());
                break;
            }
        }
    }
    schema = std::make_unique<avro::ValidSchema>(recordSchema);
}

void avroWriter::initCompression(writeOption option)
{
    if (option.compression == CompressType::UNCOMPRESS)
    {
        compression = avro::NULL_CODEC;
    }
    else if (option.compression == CompressType::SNAPPY)
    {
        compression = avro::SNAPPY_CODEC;
    }
    else
    {
        elog(ERROR, "Avro does not support this compression format.");
    }
}
