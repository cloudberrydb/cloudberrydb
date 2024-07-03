#ifndef DATALAKE_TEXTFILESNAPPYREAD_H
#define DATALAKE_TEXTFILESNAPPYREAD_H

#include "textFileInput.h"

namespace Datalake {
namespace Internal {


class textFileSnappyRead : public textFileInput {

public:
    textFileSnappyRead();

    ~textFileSnappyRead();

    virtual void open(ossFileStream ossFile, std::string fileName, readOption options);

    virtual int64_t read(void* buffer, size_t length);

    virtual void close();

    virtual int64_t seek(int64_t offset) {
        return 0;
    }

    virtual bool allowSeek() {
        return false;
    }

    virtual fileState getState() {
        return state;
    }

	virtual bool isCompress() {
		return true;
	}

    virtual void setFileLength(int64_t length) {
        fileLength = length;
    }

    virtual bool isReadFinish() {
        return readFinish;
    }

private:
    int64_t fillInputBuffer(int length);
    void resizeOutputBuffer(int size);
    void resizeInputBuffer(int size);
    void seekIntextfile(int64_t posn);
    int64_t getNext();
    int convertBigEndianToInt(char *buffer);

private:
    /* offset in file */
    int64_t pos;
    /* hdfs snappy block size */
    int64_t blockLength;
    /* uncompress size in a snappy block */
    int64_t uncompress_length_in_block;
    std::string path;
	fileState state;
    ossFileStream stream;
    datalakeMemoryBuffer inputBuffer;
    datalakeMemoryBuffer outputBuffer;
    int64_t curLeftBytesd;
    int64_t fileLength;
    bool readFinish;
    int64_t originalBlockSize;

};


}
}




#endif