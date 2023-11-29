#ifndef DATALAKE_ARCHIVEFILEREAD_H
#define DATALAKE_ARCHIVEFILEREAD_H

#include "src/common/rewrLogical.h"

namespace Datalake {
namespace Internal {

#define ARCHIEVE_READ_BUFFER_SIZE (65535)

static char archiveReadBuff[ARCHIEVE_READ_BUFFER_SIZE];

class archiveFileRead
{
public:
	archiveFileRead()
	{
		state = FILE_UNDEF;
	}

	void open(ossFileStream ossFile, std::string fileName, readOption options);

	int64_t read(void* buffer, size_t length);

	void close();

	fileState getState()
    {
        return state;
    }

private:

	static ssize_t read_call_back(struct archive *a, void *client_data, const void **buffer);

	void setArchiveReadSupport(const char* fileName);

	readOption options;

	struct archive *archive;

  	struct archive_entry *archive_entry;

	std::string fileName;

	fileState state;

};

}
}


#endif