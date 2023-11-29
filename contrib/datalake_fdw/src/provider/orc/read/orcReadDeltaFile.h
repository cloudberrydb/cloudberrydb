#ifndef DATALAKE_ORCREADDELTAFILE_H
#define DATALAKE_ORCREADDELTAFILE_H

#include <map>
#include <string>
#include <iostream>
#include <memory>
#include "orcReadInterface.h"
#include "src/common/readPolicy.h"

namespace Datalake {
namespace Internal {

typedef enum compareInternalCode
{
    EMPTY_MAP = 0,
    ROW_EQUAL_OTHER,
    ROW_GREATETHAN_OTHER,
    ROW_LESSTHAN_OTHER,
    ROW_ISNULL
} compareInternalCode;

typedef struct transcationStruct
{
    int64_t originalTransaction;
    int bucketId;
    int64_t rowId;
    int64_t currentTransaction;
    int operation;

    bool operator==(transcationStruct const &other) const
    {
        if (originalTransaction == other.originalTransaction &&
            bucketId == other.bucketId &&
            rowId == other.rowId &&
            currentTransaction == other.currentTransaction &&
            operation == other.operation)
        {
            return true;
        }
        return false;
    }

    bool operator<(transcationStruct const &other) const
    {
        if (originalTransaction < other.originalTransaction)
        {
            return true;
        }
        else if (originalTransaction == other.originalTransaction &&
                 bucketId < other.bucketId)
        {
            return true;
        }
        else if (originalTransaction == other.originalTransaction &&
                 bucketId == other.bucketId &&
                 rowId < other.rowId)
        {
            return true;
        }
        else if (originalTransaction == other.originalTransaction &&
                 bucketId == other.bucketId &&
                 rowId == other.rowId &&
                 currentTransaction > other.currentTransaction)
        {
            return true;
        }
        else
        {
            return false;
        }
    }

    bool operator>(transcationStruct const &other) const
    {
        if (originalTransaction > other.originalTransaction)
        {
            return true;
        }
        else if (originalTransaction == other.originalTransaction &&
                 bucketId > other.bucketId)
        {
            return true;
        }
        else if (originalTransaction == other.originalTransaction &&
                 bucketId == other.bucketId &&
                 rowId > other.rowId)
        {
            return true;
        }
        else if (originalTransaction == other.originalTransaction &&
                 bucketId == other.bucketId &&
                 rowId == other.rowId &&
                 currentTransaction < other.currentTransaction)
        {
            return true;
        }
        else
        {
            return false;
        }
    };
} transcationStruct;

class orcReadDeltaFile
{
public:

    void readDeleteDeltaLists(void* opt, std::vector<ListContainer> &deleteDeltaLists, bool enableCache);

    int compareTo(transcationStruct others);

    bool compareMapIsEmpty();

	void reset()
	{
		deleteDeltaMap.clear();
	}

private:
    void updateRow();

    int compareToInternal(transcationStruct cur, transcationStruct others);

    std::map<transcationStruct, std::shared_ptr<orcReadInterface>> deleteDeltaMap;
};

}
}
#endif
