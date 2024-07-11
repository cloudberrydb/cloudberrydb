#include "archiveRead.h"

extern "C" {
	#include "src/common/random_segment.h"
}

namespace Datalake {
namespace Internal {

void archiveRead::createHandler(void *sstate)
{
	initParameter(sstate);
	bool exec = createPolicy();
	if (exec)
		initFileStream();
	readNextFile();
}

bool archiveRead::createPolicy()
{
	std::vector<ListContainer> lists;
	extraFragmentLists(lists, scanstate->fragments);

	bool exec = false;
	int dummy_segid = 0;
	int dummy_segnums = 0;
	exec_segment(selected_segments, segId, segnum, &exec, &dummy_segid, &dummy_segnums);
	if (!exec)
		lists.clear();

	readPolicy.accordingConsistentHash(dummy_segid, dummy_segnums, lists);
	blockSerial = 0;

	return exec;
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
	releaseResources();
}

fileState archiveRead::getFileState()
{
	return fileRead.getState();
}

}
}