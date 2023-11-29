#include "orcReadInterface.h"
#include "orcReadDeltaFile.h"
#include <list>
#include <cassert>
#include <sstream>


namespace Datalake {
namespace Internal {


bool orcReadInterface::createORCReader(void* opt, std::string filename, int64_t length, bool enableCache, char *allocBuffer)
{
    auto istream = ORC_UNIQUE_PTR<orc::InputStream>(new OssInputStream(opt, filename, length, enableCache, allocBuffer));
    stream = (OssInputStream *)istream.get();
    if (!stream->checkORCFile())
    {
        /* this file is not orc file skip this file. */
        return false;
    }
    reader = orc::createReader(std::move(istream), options);
    return true;
}

void orcReadInterface::setRowReadOptions(std::set<int> attr_used, int column)
{
    if (!transactionTable)
    {
        std::list<uint64_t> include;
        for (int i = 0; i < column; ++i)
        {
            if (attr_used.find(i) != attr_used.end())
            {
                include.push_back(i);
            }
        }
        rowOptions.include(include);
    }
}

void orcReadInterface::createRowReader(uint64_t stripeOffset, uint64_t stripteLength)
{
    rowOptions.range(stripeOffset, stripteLength);
    rowReader = reader->createRowReader(rowOptions);
    batch = rowReader->createRowBatch(ORC_BATCH_SIZE);
}

void orcReadInterface::setTransactionTable(bool transaction)
{
    transactionTable = transaction;
}

void orcReadInterface::getStripes()
{
    for (uint64_t i = 0; i < reader->getNumberOfStripes(); ++i)
    {
        stripes.push_back(reader->getStripe(i));
    }
}

void orcReadInterface::getTransactionTableType()
{
    if (transactionTable)
    {
        const orc::Type *orcType = &reader->getType();
        type = orcType->getSubtype(5);
    }
    else
    {
        type = &reader->getType();
    }
}

orc::StructVectorBatch *orcReadInterface::getORCStructVectorBatch()
{
    if (transactionTable)
    {
        orc::StructVectorBatch *structBatch = dynamic_cast<orc::StructVectorBatch *>(batch.get());
        orcVector = dynamic_cast<orc::StructVectorBatch *>(structBatch->fields[5]);
    }
    else
    {
        orcVector = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    }
    return orcVector;
}

void orcReadInterface::deleteORCReader()
{
    orc::RowReader *row_read = rowReader.release();
    if (row_read)
        delete row_read;

    orc::Reader *read = reader.release();
    if (read)
        delete read;
}


bool orcReadInterface::getNextStripes()
{
    for (; stripesIndex < (int)stripes.size(); stripesIndex++)
    {
        createRowReader(stripes[stripesIndex]->getOffset(), stripes[stripesIndex]->getLength());
        stripesIndex += 1;
        return true;
    }
    return false;
}

bool orcReadInterface::getNextBatch()
{
    while (!rowReader || !rowReader->next(*batch))
    {
        if (!getNextStripes())
        {
            batch.reset();
            return false;
        }
    }
    return true;

}

bool orcReadInterface::readRow(void *data)
{
    while (batch)
    {
		int count = batch->numElements;
        if (rowIndex < count)
        {
            parseTranscationRow(data);
            return true;
        }
        rowIndex = 0;
        getNextBatch();
    }
    return false;
}

void orcReadInterface::parseTranscationRow(void *data)
{
    orcVector = dynamic_cast<orc::StructVectorBatch *>(batch.get());
    transcationStruct *ptr = (transcationStruct*)data;
    orc::ColumnVectorBatch *mVector = orcVector->fields[0];
    orc::LongVectorBatch *vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->operation = vec->data[rowIndex];

    mVector = orcVector->fields[1];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->originalTransaction = vec->data[rowIndex];

    mVector = orcVector->fields[2];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->bucketId = vec->data[rowIndex];

    mVector = orcVector->fields[3];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->rowId = vec->data[rowIndex];

    mVector = orcVector->fields[4];
    vec = dynamic_cast<orc::LongVectorBatch *>(mVector);
    ptr->currentTransaction = vec->data[rowIndex];

    rowIndex += 1;
}

}
}






