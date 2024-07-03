#ifndef DATALAKE_TEXTFILEINPUT_H
#define DATALAKE_TEXTFILEINPUT_H

#include "src/common/rewrLogical.h"
#include <memory>

namespace Datalake {
namespace Internal {




class textFileInput {
public:
    virtual void open(ossFileStream ossFile, std::string fileName, readOption options) = 0;

    virtual int64_t read(void* buffer, size_t length) = 0;

    virtual void close() = 0;

    virtual int64_t seek(int64_t offset) = 0;

    virtual bool allowSeek() = 0;

    virtual fileState getState() = 0;

	virtual bool isCompress() = 0;

    virtual void setFileLength(int64_t length) = 0;

    virtual bool isReadFinish() = 0;


};

std::shared_ptr<textFileInput> getTextFileInput(CompressType compress);



}
}

#endif