#include "orcReadDeltaFile.h"

namespace Datalake {
namespace Internal {

void orcReadDeltaFile::readDeleteDeltaLists(void* opt, std::vector<ListContainer> &deleteDeltaLists, bool enableCache)
{
    for (auto it = deleteDeltaLists.begin(); it != deleteDeltaLists.end(); it++)
    {
        std::shared_ptr<orcReadInterface> reader(new orcReadInterface());
        if (!reader->createORCReader(opt, it->keyName, it->size, enableCache, NULL))
        {
            continue;
        }
        transcationStruct row;
        reader->getNextBatch();
        reader->readRow((void *)&row);
        deleteDeltaMap.insert(make_pair(row, reader));
    }
}

bool orcReadDeltaFile::compareMapIsEmpty()
{
    if (deleteDeltaMap.empty())
    {
        return true;
    }
    return false;
}

int orcReadDeltaFile::compareTo(transcationStruct others)
{
    while (true)
    {
        if (deleteDeltaMap.empty())
        {
            return EMPTY_MAP;
        }

        auto it = deleteDeltaMap.begin();
        int code = compareToInternal(it->first, others);
        if (code == ROW_EQUAL_OTHER)
        {
            // this row is delete we need update row
            updateRow();
            return ROW_EQUAL_OTHER;
        }
        else if (code == ROW_LESSTHAN_OTHER)
        {
            // need read next row
            updateRow();
        }
        else if (code == ROW_GREATETHAN_OTHER)
        {
            // need base or delta to read next row
            return ROW_GREATETHAN_OTHER;
        }
        else
        {
            return code;
        }
    }
}

int orcReadDeltaFile::compareToInternal(transcationStruct cur, transcationStruct others)
{
    if (cur.originalTransaction != others.originalTransaction)
    {
        return cur.originalTransaction < others.originalTransaction ? ROW_LESSTHAN_OTHER : ROW_GREATETHAN_OTHER;
    }
    if (cur.bucketId != others.bucketId)
    {
        return cur.bucketId < others.bucketId ? ROW_LESSTHAN_OTHER : ROW_GREATETHAN_OTHER;
    }
    if (cur.rowId != others.rowId)
    {
        return cur.rowId < others.rowId ? ROW_LESSTHAN_OTHER : ROW_GREATETHAN_OTHER;
    }
    return ROW_EQUAL_OTHER;
}

void orcReadDeltaFile::updateRow()
{
    auto it = deleteDeltaMap.begin();
    std::shared_ptr<orcReadInterface> reader = it->second;
    transcationStruct row;
    if (reader->readRow((void *)&row))
    {
        deleteDeltaMap.erase(it);
        deleteDeltaMap.insert(make_pair(row, reader));
    }
    else
    {
        /* Read the end. erase map first */
        reader->deleteORCReader();
        deleteDeltaMap.erase(it);
    }
}

}
}