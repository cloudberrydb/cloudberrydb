#ifndef DATALAKE_TEXTFILEREADPOLICY_H
#define DATALAKE_TEXTFILEREADPOLICY_H

#include "src/common/rewrLogical.h"
#include "src/provider/provider.h"

namespace Datalake {
namespace Internal {

class textFileReadPolicy : public readBlockPolicy{
public:

    void textFileBuild(int segId, int segNum,  int blockSize, std::vector<ListContainer> lists, ossFileStream ossFile, readOption options);

    CompressType checkingFileMode(std::string filename, int64_t size, ossFileStream ossFile, readOption options);

    bool isCompressionFormat(const std::string& filename, std::string compressionFormat);

    bool isArchiveFileCompression(std::string filename, ossFileStream ossFile, readOption options);

    void buildSmallFile(int64_t index, std::string &name, int64_t size, CompressType compress);

    void insertNewBlock(int64_t index, std::string &name, int64_t size, 
    int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeOffsetEnd, CompressType compress);

    void updateOldBlock(int64_t index, std::string &name, int64_t size, 
    int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeOffsetEnd, CompressType compress);

    void buildBigFile(int64_t &index, std::string &name, int64_t size, CompressType compress);
};

}
}

#endif
