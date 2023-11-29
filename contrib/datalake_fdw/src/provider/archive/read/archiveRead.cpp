#include "archiveRead.h"

namespace Datalake {
namespace Internal {

void archiveRead::createHandler(void *sstate)
{
	initParameter(sstate);
	createPolicy();
	readNextFile();
}

void archiveRead::createPolicy()
{
	std::vector<ListContainer> lists;
	extraFragmentLists(lists, scanstate->fragments);
	readPolicy.accordingConsistentHash(segId, segnum, lists);
	blockSerial = 0;
}

bool archiveRead::readNextFile()
{
	if (blockSerial >= (int)readPolicy.readingLists.size())
	{
		return false;
	}
	metaInfo info = readPolicy.readingLists[blockSerial];
	fileRead.open(fileStream, info.fileName, options);
	blockSerial++;
	return true;
}

int64_t archiveRead::readWithBuffer(void* buffer, int64_t length)
{
	if (getFileState() != FILE_OPEN)
	{
		/* no new file open read over */
		return 0;
	}
	int64_t size = fileRead.read(buffer, length);
	if (size == 0)
	{
		fileRead.close();
		if (!readNextFile())
		{
			/* read over */
			return 0;
		}
		size = fileRead.read(buffer, length);
	}
	return size;
}

void archiveRead::destroyHandler()
{
	fileRead.close();
}

fileState archiveRead::getFileState()
{
	return fileRead.getState();
}

}
}