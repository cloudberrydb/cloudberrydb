#include <parquet/arrow/util/decimal.h>
#include <parquet/arrow/result.h>
#include <avro/Generic.hh>

#include "avroRead.h"
#include "avroOssInputStream.h"

extern "C"
{
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "fmgr.h"
}

#define AVRO_READ_BUFFER_SIZE (8 * 1024 * 1024)

namespace Datalake{
namespace Internal{

void avroRead::createHandler(void *sstate)
{
    initParameter(sstate);
    initializeColumnValue();
    createPolicy();
    readBuffer_ = std::make_unique<AvroStreamBuffer>(AVRO_READ_BUFFER_SIZE);
    readNextGroup();
}

void avroRead::restart()
{
    rowGroupNums.clear();
    tempRowGroupNums.clear();
    curRowGroupNum = 0;
    last = false;
    fileReader.reset();
    blockPolicy.resetReadBlockPolicy();
    initializeColumnValue();
    createPolicy();
    readNextGroup();
}

bool avroRead::checkSchema(const avro::ValidSchema &dataSchema)
{
    const avro::NodePtr &data = dataSchema.root();
    nullBranches.clear();
    if (data->type() != avro::AVRO_RECORD)
    {
        return false;
    }
    AttrNumber dataColumn = data->leaves();
    if (ncolumns != dataColumn + nPartitionKey)
    {
        return false;
    }
    for (int i = 0; i < dataColumn; ++i)
    {
        const avro::NodePtr &leaf = data->leafAt(i);
        Oid typeId = tupdesc->attrs[i].atttypid;
        std::string columnName = tupdesc->attrs[i].attname.data;
        nullBranches.push_back(getNullBranchIdx(leaf));
        switch (typeId)
        {
            case BOOLOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_BOOL, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case INT2OID: 
            case INT4OID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_INT, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case INT8OID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case FLOAT4OID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_FLOAT, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case FLOAT8OID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_DOUBLE, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case NUMERICOID:
            {
                int precision = 0;
                int scale = 0;
                if (tupdesc->attrs[i].atttypmod < 0)
                {
                    precision = AVRO_NUMERIC_DEFAULT_SIZE;
                    scale = 0;
                }
                else
                {
                    precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
                    scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
                    if (precision > AVRO_NUMERIC_MAX_SIZE)
                    {
                        elog(ERROR, "The precision of Avro decimal should be less than %d", AVRO_NUMERIC_MAX_SIZE);
                    }
                }
                avro::LogicalType ltype(avro::LogicalType::DECIMAL);
                ltype.setPrecision(precision);
                ltype.setScale(scale);
                if (!checkDataTypeCompatible(leaf, avro::AVRO_FIXED, ltype) && !checkDataTypeCompatible(leaf, avro::AVRO_BYTES, ltype))
                {
                    return false;
                }
                break;
            }
            case BYTEAOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_BYTES, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case CHAROID:
            case BPCHAROID:
            case VARCHAROID:
            case TEXTOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_STRING, avro::LogicalType(avro::LogicalType::NONE)))
                {
                    return false;
                }
                break;
            }
            case DATEOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_INT, avro::LogicalType(avro::LogicalType::DATE)))
                {
                    return false;
                }
                break;
            }
            case TIMEOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIME_MICROS))
                    && !checkDataTypeCompatible(leaf, avro::AVRO_INT, avro::LogicalType(avro::LogicalType::TIME_MILLIS)))
                {
                    return false;
                }
                break;
            }
            case TIMESTAMPOID:
            case TIMESTAMPTZOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MICROS))
                    && !checkDataTypeCompatible(leaf, avro::AVRO_LONG, avro::LogicalType(avro::LogicalType::TIMESTAMP_MILLIS)))
                {
                    return false;
                }
                break;
            }
            case UUIDOID:
            {
                if (!checkDataTypeCompatible(leaf, avro::AVRO_STRING, avro::LogicalType(avro::LogicalType::UUID)))
                {
                    return false;
                }
                break;
            }
        }
    }
    bufDatum_ = std::make_unique<avro::GenericDatum>(dataSchema);
    return true;
}

void avroRead::createPolicy()
{
    std::vector<ListContainer> lists;
    if (scanstate->options->hiveOption->hivePartitionKey != NULL)
    {
        List* fragment =  (List*)list_nth(scanstate->fragments, scanstate->options->hiveOption->curPartition);
        extraFragmentLists(lists, fragment);
        scanstate->options->hiveOption->curPartition += 1;
    }
    else
    {
        extraFragmentLists(lists, scanstate->fragments);
    }
    blockPolicy.build(segId, segnum, BLOCK_POLICY_SIZE, lists);
    blockPolicy.distBlock();
    blockSerial = blockPolicy.start;
}

bool avroRead::getNextGroup()
{
    int size = tempRowGroupNums.size();
    for (; curRowGroupNum < size; curRowGroupNum++)
    {
        fileReader->readDataBlock(tempRowGroupNums[curRowGroupNum]);
        curRowGroupNum++;
        return true;
    }
    return false;
}

bool avroRead::readNextFile()
{
    if (blockSerial > blockPolicy.end)
    {
        return false;
    }
    tempRowGroupNums.clear();
    curRowGroupNum = 0;
    auto it = blockPolicy.block.find(blockSerial);
    if (it != blockPolicy.block.end())
    {
        metaInfo info = it->second;
        if (info.exists)
        {
            if (info.fileLength <= info.blockSize)
            {
                return getRowGropFromSmallFile(info);
            }
            else
            {
                return getRowGropFromBigFile(info);
            }
        }
    }
    else
    {
        int64_t blockCount = blockPolicy.block.size();
        elog(ERROR, "external table internal error. block index %d "
        "not found in avro block policy. block count %ld", blockSerial, blockCount);
    }
    return true;
}

bool avroRead::createBlockReader()
{
    try
    {
        fileReader.reset();
        fileReader = std::make_unique<avroBlockReader>(std::make_unique<avroOssInputStream>(fileStream, curFileName, readBuffer_->size_, readBuffer_->buf, options.enableCache), curFileName);
    }
    catch (avro::Exception &e)
    {
        elog(LOG, "Create AvroBlockReader failed. %s Avro file name: %s.", e.what(), curFileName.c_str());
        return false;
    }
    if (!checkSchema(fileReader->dataSchema())){
        elog(ERROR, "Schema mismatch! Avro file name: %s. External type mapping: %s", curFileName.c_str(), getTypeMappingSupported().c_str());
        return false;
    }
    if (!fileReader->getDataBlockStartList(rowGroupNums))
    {
        elog(LOG, "Sync marker mismatch! Avro file name: %s.", curFileName.c_str());
        return false;
    }
    return true;
}

bool avroRead::getRowGropFromSmallFile(metaInfo info)
{
    curFileName = info.fileName;
    if (!createBlockReader())
    {
        //this file format not avro skip it.
        elog(LOG, "DataLake LOG, file %s format is not avro and skip it.", info.fileName.c_str());
        return true;
    }
    tempRowGroupNums.swap(rowGroupNums);
    curRowGroupNum = 0;
    return true;
}

bool avroRead::getRowGropFromBigFile(metaInfo info)
{
    if (curFileName != info.fileName)
    {
        // open next file
        curFileName = info.fileName;
        if (!createBlockReader())
        {
            //this file format not avro skip it.
            elog(LOG, "DataLake LOG, file %s format is not avro and skip it.", info.fileName.c_str());
            return true;
        }
        
        tempRowGroupNums.clear();
        curRowGroupNum = 0;
    }

    size_t count = rowGroupNums.size();
    for (size_t i = 0; i < count; i++)
    {
        int64_t offset = rowGroupNums[i];
        if (offset > info.rangeOffsetEnd)
        {
            break;
        }
        if (info.rangeOffset <= offset && offset <= info.rangeOffsetEnd)
        {
            tempRowGroupNums.push_back(offset);
        }
    }
    return true;
}

fileState avroRead::getFileState()
{
    return state_;
}

int64_t avroRead::read(void *values, void *nulls)
{
nextPartition:

    if (readNextRow((Datum*)values, (bool*)nulls))
    {
        if (scanstate->options->hiveOption->hivePartitionKey != NULL)
        {
            AttrNumber numDefaults = this->nDefaults;
            int *defaultMap = this->defMap;
            ExprState **defaultExprs = this->defExprs;

            for (int i = 0; i < numDefaults; i++)
            {
                /* only eval const expr, so we don't need pg_try catch block here */
                Datum* value = (Datum*)values;
                bool* null = (bool*)nulls;
                value[defaultMap[i]] = ExecEvalConst(defaultExprs[i], NULL, &null[defaultMap[i]], NULL);
            }
        }
        return 1;
    }

    if (!isLastPartition(scanstate))
    {
        restart();
        goto nextPartition;
    }
    return 0;
}

bool avroRead::getRow(Datum *values, bool *nulls)
{
    return convertToDatum(values, nulls);
}

bool avroRead::convertToDatum(Datum *values, bool *nulls)
{
    if (fileReader != nullptr && fileReader->hasMoreData())
    {
        fileReader->decr();
        avro::decode(fileReader->decoder(), *bufDatum_);
        AttrNumber nColumnsToRead = ncolumns - nPartitionKey;
        for (AttrNumber i = 0; i < nColumnsToRead; ++i)
        {
            if (i >= (AttrNumber) fileReader->getColumns())
            {
                nulls[i] = true;
                continue;
            }
            if (!options.includes_columns[i])
            {
                nulls[i] = true;
                continue;
            }
            if (!getNextColumn(values[i], bufDatum_->value<avro::GenericRecord>(), i, nulls[i]))
            {
                elog(ERROR, "read column %d failed", i);
                return false;
            }
        }
        return true;
    }
    return false;
}

bool avroRead::getNextColumn(Datum &values, avro::GenericRecord &record, int idx, bool &isNull)
{
    avro::GenericDatum &datum = record.fieldAt(idx);
    Oid typeOid = tupdesc->attrs[idx].atttypid;
    if (datum.isUnion() && (int8_t)datum.unionBranch() == nullBranches[idx])
    {
        isNull=true;
        return true;
    }
    isNull = false;
    switch (typeOid)
    {
        case BOOLOID:
        {
            values = BoolGetDatum(datum.value<bool>());
            break;
        }
        case INT2OID:
        case INT4OID:
        {
            values = Int32GetDatum(datum.value<int32_t>());
            break;
        }
        case INT8OID:
        {
            values = Int64GetDatum(datum.value<int64_t>());
            break;
        }
        case FLOAT4OID:
        {
            values = Float4GetDatum(datum.value<float>());
            break;
        }
        case FLOAT8OID:
        {
            values = Float8GetDatum(datum.value<double>());
            break;
        }
        case BYTEAOID:
        {
            std::vector<uint8_t> &res = datum.value<std::vector<uint8_t>>();
            int64_t datalen = res.size();
            bytea *result = (bytea*)palloc(datalen + VARHDRSZ);
            memcpy(VARDATA(result), res.data(), datalen);
            SET_VARSIZE(result, datalen + VARHDRSZ);
            values = PointerGetDatum(result);
            break;
        }
        case CHAROID:
        {
            std::string &res = datum.value<std::string>();
            int64_t datalen = res.size();
            char result[5] = {0};
            memcpy(result, res.data(), datalen);
            values = CharGetDatum(result[0]);
            break;
        }
        case BPCHAROID:
        case VARCHAROID:
        case TEXTOID:
        {
            std::string &res = datum.value<std::string>();
            int64_t datalen = res.size();
            bytea *result = (bytea*)palloc(datalen + VARHDRSZ);
            memcpy(VARDATA(result), res.data(), datalen);
            SET_VARSIZE(result, datalen + VARHDRSZ);
            values =  PointerGetDatum(result);
            break;
        }
        case DATEOID:
        {
            values = DateADTGetDatum(datum.value<int32_t>() + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
            break;
        }
        case TIMEOID:
        {
            if (datum.logicalType().type() == avro::LogicalType::TIME_MILLIS)
            {
                values = TimestampGetDatum(datum.value<int32_t>());
            }
            else if (datum.logicalType().type() == avro::LogicalType::TIME_MICROS)
            {
                values = TimestampGetDatum(datum.value<int64_t>());
            }
            break;
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        {
            if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MILLIS)
            {
                values = TimestampGetDatum((datum.value<int64_t>() + (int64_t)(UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) * 60 * 60 * 24 * 1000) * 1000);
            }
            else if (datum.logicalType().type() == avro::LogicalType::TIMESTAMP_MICROS)
            {
                values = TimestampGetDatum(datum.value<int64_t>() + (int64_t)(UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE) * 60 * 60 * 24 * 1000000);
            }
            break;
        }
        case NUMERICOID:
        {
            int precision = datum.logicalType().precision();
            int scale = datum.logicalType().scale();
            const std::vector<uint8_t> *res = nullptr;
            if (datum.type() == avro::AVRO_BYTES)
            {
                res = &datum.value<std::vector<uint8_t>>();
            }
            else if (datum.type() == avro::AVRO_FIXED)
            {
                res = &datum.value<avro::GenericFixed>().value();
            }
            else
            {
                elog(ERROR, "Primitive type of decimal is not AVRO_BYTES or AVRO_FIXED.");
            }

            if (precision <= AVRO_NUMERIC_128_SIZE)
            {
                parquet_arrow::Result<parquet_arrow::Decimal128> result = parquet_arrow::Decimal128::FromBigEndian(res->data(), res->size());
                if (!result.ok())
                {
                    elog(ERROR, "decimal128: out of range");
                }
                parquet_arrow::Decimal128 decimal = result.ValueOrDie();
                values = DirectFunctionCall3(numeric_in, CStringGetDatum(decimal.ToString(scale).c_str()), ObjectIdGetDatum(0), Int32GetDatum(tupdesc->attrs[idx].atttypmod));
            }
            else
            {
                elog(ERROR, "avro decimal is out of range, the precision should less than or equal to %d", AVRO_NUMERIC_MAX_SIZE);
            }

            break;
        }
        case UUIDOID:
        {
            values = DirectFunctionCall1(uuid_in, CStringGetDatum(datum.value<std::string>().c_str()));
            break;
        }
        default:
        {
            elog(ERROR, "unsupported type %d", typeOid);
            return false;
        }
    }
    return true;
}

void avroRead::destroyHandler()
{
    fileReader.reset();
    destroyFileSystem(fileStream);
    fileStream = nullptr;
}

}
}
