#ifndef DATALAKE_TEXTFILEDEFLATEREAD_H
#define DATALAKE_TEXTFILEDEFLATEREAD_H

#include <zlib.h>
#include "textFileInput.h"
// #include "src/Provider/common/datalakeBuffer.h"
#include "src/common/datalakeBuffer.h"

namespace Datalake {
namespace Internal {

#define ZLIB_CHUNK_SIZE (65536)

class textFileDeflateRead : public textFileInput {

public:
    textFileDeflateRead();

    ~textFileDeflateRead();

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
    void resizeZlibChunkBuffer(int size);

    void resizeInputBuffer(int size);

    void resizeOutputBuffer(int size);

    int64_t getNext();

    int64_t fillInputBuffer(int length);

private:
    int pos;
    std::string path;
	fileState state;
    ossFileStream stream;
    datalakeByteBuffer* inputBuffer;
    datalakeByteBuffer* zlibChunkBuffer;
    datalakeByteBuffer* outputBuffer;
    int64_t curLeftBytesd;
    int64_t fileLength;
    bool readFinish;
    z_stream strm;


};

}
}

#endif