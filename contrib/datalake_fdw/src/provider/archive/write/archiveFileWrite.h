#ifndef DATALAKE_ARCHIVEFILEWRITE_H
#define DATALAKE_ARCHIVEFILEWRITE_H

#include "src/common/rewrLogical.h"

namespace Datalake {
namespace Internal {



class archiveFileWrite {
public:
	archiveFileWrite()
	{
		archive = NULL;
		archive_entry = NULL;
	}

	void open(ossFileStream ossFile, std::string fileName, void *sstate, writeOption option);

	int64_t write(const void* buf, size_t length);

	static ssize_t write_call_back(struct archive * arch, void *myData, const void *buffer, size_t length);

	void close();

private:
	std::string name;
	writeOption option;
	ossFileStream ossFile;

	struct archive *archive;
	struct archive_entry *archive_entry;
};



}
}


#endif