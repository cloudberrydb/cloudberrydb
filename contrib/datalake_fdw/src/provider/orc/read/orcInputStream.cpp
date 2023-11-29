#include <list>
#include <cassert>
#include <sstream>
#include "orcInputStream.h"

extern "C" {
#include "utils/elog.h"
#include "src/datalake_def.h"
}

namespace Datalake {
namespace Internal {

OssInputStream::OssInputStream(void* opt, std::string filename, uint64_t length, bool enableCache, char* allocBuffer)
	: filename(filename), length(length), enableCache(enableCache), allocBuffer(allocBuffer)
{
	dataLakeOptions* datalakeOption = (dataLakeOptions*)opt;
	gopherConfig* conf = createGopherConfig((void*)(datalakeOption->gopher));
	stream = createFileSystem(conf);
	freeGopherConfig(conf);

	int flag = O_RDONLY | O_RDTHR;
	if (enableCache)
	{
		flag = O_RDONLY;
	}
	openFile(stream, filename.c_str(), flag);
}

OssInputStream::~OssInputStream()
{
	closeFile(stream);
}

void OssInputStream::range(uint64_t offset, uint64_t length)
{
	/* If the orc is not very efficient. Maybe can do a memory load in the range */
	//TODO
}

void OssInputStream::read(void *buf, uint64_t length, uint64_t offset)
{
	seekFile(stream, offset);
	readFile(stream, buf, length);
}

bool OssInputStream::checkORCFile()
{
    // skip hive orc acid version
    int pos = filename.find("orc_acid_version");
    if (pos >= 0)
    {
        return false;
    }

    // Check ORC Magic
	seekFile(stream, 0);
	char magic[4] = {0};
	readFile(stream, magic, 3);
    if (strcmp(magic, "ORC") != 0)
    {
        elog(WARNING, "%s is not orc file.", filename.c_str());
		return false;
    }
	seekFile(stream, 0);
    return true;
}

}
}