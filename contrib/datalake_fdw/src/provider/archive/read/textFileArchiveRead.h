#ifndef DATALAKE_TEXTFILEARCHIVEREAD_H
#define DATALAKE_TEXTFILEARCHIVEREAD_H

#include "textFileInput.h"
#include <iostream>

namespace Datalake {
namespace Internal {

#define ARCHIEVE_READ_BUFFER_SIZE (65535)

class textFileArchiveRead : public textFileInput {
public:
	textFileArchiveRead()
	{
		state = FILE_UNDEF;
	}

	virtual void open(ossFileStream ossFile, std::string fileName, readOption options);

	virtual int64_t read(void* buffer, size_t length);

    virtual int64_t seek(int64_t offset);

    virtual bool allowSeek();

	virtual void close();

	virtual fileState getState() {
        return state;
    }

	virtual bool isCompress();

    virtual void setFileLength(int64_t length) {}

    virtual bool isReadFinish() {
        return false;
    }

private:

	static ssize_t read_call_back(struct archive *a, void *client_data, const void **buffer);
	static ssize_t archive_seek_callback(struct archive *, void *_client_data, ssize_t offset, int whence);
    
    void setArchiveReadSupport(const char* fileName);

private:
    struct archive *archive;
  	struct archive_entry *archive_entry;
	std::string fileName;
	fileState state;
    int archive_format_tag;

};

}
}


#endif