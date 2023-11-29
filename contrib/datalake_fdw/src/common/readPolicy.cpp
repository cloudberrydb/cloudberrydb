#include "readPolicy.h"
#include "makeHash.h"

extern "C"
{
#include "utils/elog.h"
}

namespace Datalake {
namespace Internal {

std::string readPolicyBase::explainPolicy()
{
	return "";
}

void readBlockPolicy::build(int segId, int segNum, int blockSize, std::vector<ListContainer> lists)
{
	int64_t index = 0;
    this->blockSize = blockSize;
    this->segId = segId;
    this->segNum = segNum;
	filterList(lists);
	for (auto it = normalLists.begin(); it != normalLists.end(); it++)
    {
        if (it->size <= blockSize)
        {
            buildSmallFile(index, it->keyName, it->size);
            index += 1;
        }
        else
        {
            buildBigFile(index, it->keyName, it->size);
        }
    }

    int64_t remainderSize = block.size() % segNum;
    if (remainderSize != 0)
    {
        for (int64_t i = 0; i < segNum - remainderSize; i++)
        {
            std::string name = "";
            insertNewBlock(index, name, 0, blockSize, false, 0, 0);
            index += 1;
        }
    }
}

void readBlockPolicy::filterList(std::vector<ListContainer> lists)
{
	for (auto it = lists.begin(); it != lists.end(); it++)
	{
		ListContainer content;
		content.keyName = it->keyName;
		content.size = it->size;
		normalLists.push_back(content);
	}
}

void readBlockPolicy::distBlock()
{
	if (segNum <= 0)
    {
        elog(ERROR, "distribute block get segnum is %d.", segNum);
    }
    int64_t num = block.size() / segNum;
    if (num < 0)
    {
        int64_t count = block.size();
        elog(ERROR, "block count less than segnum. distribute Block size count %ld segnum %d segid %d", count, segNum, segId);
    }
    // segid num begin is 0
    start = (segId) * (num);
    end = (segId + 1) * num - 1;
}

std::string readBlockPolicy::explainPolicy()
{
	return "";
}

void readBlockPolicy::buildBigFile(int64_t &index, std::string &name, int64_t size)
{
    int64_t remainderSize = size % blockSize;
    if (remainderSize == 0)
    {
        for (int64_t offset = 0; offset < size; offset += blockSize)
        {
            insertNewBlock(index, name, size, blockSize, true, offset, offset + blockSize);
            index += 1;
        }
    }
    else
    {
        int64_t halfSize = blockSize / 2;
        int64_t offset = 0;
        int64_t remainderLength = size;
        for (; remainderLength > blockSize; remainderLength -= blockSize)
        {
            insertNewBlock(index, name, size, blockSize, true, offset, offset + blockSize);
            index += 1;
            offset += blockSize;
        }

        if (remainderLength > halfSize)
        {
            insertNewBlock(index, name, size, blockSize, true, offset, offset + remainderLength);
            index += 1;
        }
        else
        {
            index -= 1;
            updateOldBlock(index, name, size, blockSize, true, offset, offset + remainderLength);
            index += 1;
        }
    }
}

void readBlockPolicy::updateOldBlock(int64_t index, std::string &name, int64_t size, int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeOffsetEnd)
{
    auto it = block.find(index);
    if (it != block.end())
    {
        auto oldInfo = it->second;
        metaInfo info;
        info.blockSize = oldInfo.blockSize;
        info.exists = oldInfo.exists;
        info.fileLength = oldInfo.fileLength;
        info.fileName = oldInfo.fileName;
        info.rangeOffset = oldInfo.rangeOffset;
        info.rangeOffsetEnd = rangeOffsetEnd;
        block[index] = info;
    }
    else
    {
        elog(ERROR, "reader distribute block error. file name %s file length %ld blockSize %ld rangeOffset %ld rangeOffsetEnd %ld",
            name.c_str(), size, blockSize, rangeOffset, rangeOffsetEnd);
    }
}

void readBlockPolicy::buildSmallFile(int64_t index, std::string &name, int64_t size)
{
    metaInfo info;
    info.blockSize = blockSize;
    info.exists = true;
    info.fileLength = size;
    info.fileName = name;
    info.rangeOffset = 0;
    info.rangeOffsetEnd = size;
    block.insert(std::make_pair(index, info));
}

void readBlockPolicy::insertNewBlock(int64_t index, std::string &name, int64_t size, int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeOffsetEnd)
{
    metaInfo info;
    info.blockSize = blockSize;
    info.exists = exists;
    info.fileLength = size;
    info.fileName = name;
    info.rangeOffset = rangeOffset;
    info.rangeOffsetEnd = rangeOffsetEnd;
    block.insert(std::make_pair(index, info));
}

void orcReadPolicy::build(int segId, int segNum,  int blockSize, std::vector<ListContainer> lists)
{
	int64_t index = 0;
    this->blockSize = blockSize;
    this->segId = segId;
    this->segNum = segNum;
	filterList(hiveTranscation, lists);

	for (auto it = normalLists.begin(); it != normalLists.end(); it++)
    {
        if (it->size <= blockSize)
        {
            buildSmallFile(index, it->keyName, it->size);
            index += 1;
        }
        else
        {
            buildBigFile(index, it->keyName, it->size);
        }
    }

    int64_t remainderSize = block.size() % segNum;
    if (remainderSize != 0)
    {
        for (int64_t i = 0; i < segNum - remainderSize; i++)
        {
            std::string name = "";
            insertNewBlock(index, name, 0, blockSize, false, 0, 0);
            index += 1;
        }
    }
}

void orcReadPolicy::filterList(bool hiveTranscation, std::vector<ListContainer> lists)
{
    for (auto it = lists.begin(); it != lists.end(); it++)
    {
        ListContainer content;
        content.keyName = it->keyName;
        content.size = it->size;
        if (hiveTranscation && whetherDeleteDelta(it->keyName))
        {
            deleteDeltaLists.push_back(content);
        }
        else
        {
            normalLists.push_back(content);
        }
    }
}

bool orcReadPolicy::whetherDeleteDelta(std::string name)
{
    int pos = name.find("delete_delta");
    if (pos >= 0)
    {
        return true;
    }
    return false;
}

std::string orcReadPolicy::explainPolicy()
{
	return "";
}

void orcReadPolicy::reset()
{
	hiveTranscation = false;
	deleteDeltaLists.clear();
	resetReadBlockPolicy();
}

bool cmp(const ListContainer &val, const ListContainer &val1)
{
	if (strcmp(val.keyName.c_str(), val1.keyName.c_str()) < 0)
	{
	return true;
	}
	else if (val.keyName == val1.keyName)
	{
	return val.size < val1.size;
	}
	return false;
}

void readAccordingToFilePolicy::accordingToFilePolicy(int segId, int segNum, std::vector<ListContainer> lists)
{
	int size = lists.size();
	for (int i = segId; i < size; i += segNum)
	{
		metaInfo info;
		info.fileName = lists[i].keyName;
		info.fileLength = lists[i].size;
		readingLists.push_back(info);
	}
}

void readAccordingConsistentHash::accordingConsistentHash(int segId, int segNum, std::vector<ListContainer> lists)
{
	int size = lists.size();
	CdbHash* h = inithash(segNum);
	for (int i = 0; i < size; i++)
	{
		resethashkey(h);
		metaInfo info;
		info.fileName = lists[i].keyName;
		info.fileLength = lists[i].size;
		makehashkey(h, info.fileName.c_str());
		if (reducehash(h) == segId)
		{
			readingLists.push_back(info);
		}
	}
	freehash(h);
}

}
}