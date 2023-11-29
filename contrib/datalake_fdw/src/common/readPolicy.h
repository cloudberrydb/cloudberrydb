#ifndef DATALAKE_READPOLICY_H
#define DATALAKE_READPOLICY_H

#include <map>
#include <vector>
#include <iostream>
#include <string>


namespace Datalake {
namespace Internal {

typedef struct metaInfo
{
    bool exists;
    std::string fileName;
    int64_t fileLength;
    int64_t blockSize;
    int64_t rangeOffset;
    int64_t rangeOffsetEnd;
} metaInfo;

typedef struct ListContainer
{
	std::string keyName;
	int64_t size;
} ListContainer;

class readPolicyBase
{
public:
	virtual std::string explainPolicy();
};

class readBlockPolicy : public readPolicyBase
{
public:
	virtual void build(int segId, int segNum, int blockSize, std::vector<ListContainer> lists);

	void distBlock();

	virtual std::string explainPolicy();

	int start;

	int end;

	std::map<int, metaInfo> block;

	int64_t blockSize;

	std::string curFileName;

	std::vector<ListContainer> normalLists;

	int segId;

    int segNum;

	void buildBigFile(int64_t &index, std::string &name, int64_t size);

    void updateOldBlock(int64_t index, std::string &name, int64_t size, int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeLength);

    void insertNewBlock(int64_t index, std::string &name, int64_t size, int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeLength);

    void buildSmallFile(int64_t index, std::string &name, int64_t size);

	void resetReadBlockPolicy()
	{
		start = 0;
		end = 0;
		block.clear();
		blockSize = 0;
		curFileName = "";
		normalLists.clear();
		segId = 0;
		segNum = 0;
	}

private:
	void filterList(std::vector<ListContainer> lists);
};

class orcReadPolicy : public readBlockPolicy
{
public:
	virtual void build(int segId, int segNum,  int blockSize, std::vector<ListContainer> lists);

	virtual std::string explainPolicy();

	std::vector<ListContainer> deleteDeltaLists;

	bool hiveTranscation;

	void reset();

private:
    bool whetherDeleteDelta(std::string name);

    void filterList(bool hiveTranscation, std::vector<ListContainer> lists);
};

bool cmp(const ListContainer &val, const ListContainer &val1);

class readAccordingToFilePolicy : public readPolicyBase
{
public:

	void accordingToFilePolicy(int segId, int segNum, std::vector<ListContainer> lists);

	std::vector<metaInfo> readingLists;

	std::string curFileName;
};

class readAccordingConsistentHash : public readPolicyBase
{
public:
	void accordingConsistentHash(int segId, int segNum, std::vector<ListContainer> lists);

	std::vector<metaInfo> readingLists;

	std::string curFileName;
};

}
}
#endif