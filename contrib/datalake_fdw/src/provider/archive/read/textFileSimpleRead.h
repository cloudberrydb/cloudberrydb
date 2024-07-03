#ifndef DATALAKE_TEXTFILESIMPLEREAD_H
#define DATALAKE_TEXTFILESIMPLEREAD_H

#include "textFileInput.h"

namespace Datalake {
namespace Internal {


class textFileSimpleRead : public textFileInput {

public:
    textFileSimpleRead();

    ~textFileSimpleRead();

    virtual void open(ossFileStream ossFile, std::string fileName, readOption options);

    virtual int64_t read(void* buffer, size_t length);

    virtual void close();

    virtual int64_t seek(int64_t offset);

    virtual bool allowSeek() {
        return true;
    }

    virtual fileState getState() {
        return state;
    }

	virtual bool isCompress() {
		return false;
	}

    virtual void setFileLength(int64_t length) {}

    virtual bool isReadFinish() {
        return false;
    }

private:
    std::string fileName;
	fileState state;
    ossFileStream stream;

};


}
}




#endif