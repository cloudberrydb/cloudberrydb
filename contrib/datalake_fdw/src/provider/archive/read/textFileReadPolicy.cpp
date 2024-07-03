#include "textFileReadPolicy.h"
#include "textFileArchiveRead.h"

namespace Datalake {
namespace Internal {

bool textFileReadPolicy::isCompressionFormat(const std::string& filename, std::string compressionFormat) {
    size_t pos = filename.rfind('.');
    if (pos != std::string::npos) {
        std::string extension = filename.substr(pos);
        return extension == compressionFormat;
    }
    return false;
}

bool textFileReadPolicy::isArchiveFileCompression(std::string filename, ossFileStream ossFile, readOption options) {
    textFileArchiveRead reader;
    reader.open(ossFile, filename, options);
    bool isCompress = reader.isCompress();
    reader.close();
    return isCompress;
}

CompressType textFileReadPolicy::checkingFileMode(std::string filename, int64_t size, ossFileStream ossFile, readOption options) {
    if (isCompressionFormat(filename, ".snappy")) {
        return SNAPPY;
    } else if (isCompressionFormat(filename, ".gz")) {
        return GZIP;
    } else if (isCompressionFormat(filename, ".zip")) {
        return ZIP;
    } else if (isCompressionFormat(filename, ".deflate")) {
        return DEFLATE;
    } else if (size > blockSize) {
        return isArchiveFileCompression(filename, ossFile, options) ? ARCHIVE_SUPPORT_COMPRESS_FORMAT : UNCOMPRESS;
    }
    return ARCHIVE_SUPPORT_COMPRESS_FORMAT;
}

void textFileReadPolicy::textFileBuild(int segId, int segNum,  int blockSize, std::vector<ListContainer> lists, ossFileStream ossFile, readOption options) {
    int64_t index = 0;
    this->blockSize = blockSize;
    this->segId = segId;
    this->segNum = segNum;
	filterList(lists);
	for (auto it = normalLists.begin(); it != normalLists.end(); it++)
    {
        CompressType compress = checkingFileMode(it->keyName, it->size, ossFile, options);
        if (it->size <= blockSize || compress != UNCOMPRESS)
        {
            buildSmallFile(index, it->keyName, it->size, compress);
            index += 1;
        }
        else
        {
            buildBigFile(index, it->keyName, it->size, compress);
        }
    }

    int64_t remainderSize = block.size() % segNum;
    if (remainderSize != 0)
    {
        for (int64_t i = 0; i < segNum - remainderSize; i++)
        {
            std::string name = "";
            insertNewBlock(index, name, 0, blockSize, false, 0, 0, ARCHIVE_SUPPORT_COMPRESS_FORMAT);
            index += 1;
        }
    }
}

void textFileReadPolicy::buildSmallFile(int64_t index, std::string &name, int64_t size, CompressType compress)
{
    metaInfo info;
    info.blockSize = blockSize;
    info.exists = true;
    info.fileLength = size;
    info.fileName = name;
    info.rangeOffset = 0;
    info.rangeOffsetEnd = size;
    info.compress = compress;
    block.insert(std::make_pair(index, info));
}

void textFileReadPolicy::insertNewBlock(int64_t index, std::string &name, int64_t size, 
    int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeOffsetEnd, CompressType compress)
{
    metaInfo info;
    info.blockSize = blockSize;
    info.exists = exists;
    info.fileLength = size;
    info.fileName = name;
    info.rangeOffset = rangeOffset;
    info.rangeOffsetEnd = rangeOffsetEnd;
    info.compress = compress;
    block.insert(std::make_pair(index, info));
}

void textFileReadPolicy::updateOldBlock(int64_t index, std::string &name, int64_t size, 
    int64_t blockSize, bool exists, int64_t rangeOffset, int64_t rangeOffsetEnd, CompressType compress)
{
    auto it = block.find(index);
    if (it != block.end())
    {
        auto oldInfo = it->second;
        metaInfo info;
        info.blockSize = oldInfo.blockSize;
        info.exists = oldInfo.exists;
        info.fileLength = oldInfo.fileLength;
        info.fileName = oldInfo.fileName;
        info.rangeOffset = oldInfo.rangeOffset;
        info.rangeOffsetEnd = rangeOffsetEnd;
        info.compress = compress;
        block[index] = info;
    }
    else
    {
        elog(ERROR, "reader distribute block error. file name %s file length %ld blockSize %ld rangeOffset %ld rangeOffsetEnd %ld",
            name.c_str(), size, blockSize, rangeOffset, rangeOffsetEnd);
    }
}

void textFileReadPolicy::buildBigFile(int64_t &index, std::string &name, int64_t size, CompressType compress)
{
    int64_t remainderSize = size % blockSize;
    if (remainderSize == 0)
    {
        for (int64_t offset = 0; offset < size; offset += blockSize)
        {
            insertNewBlock(index, name, size, blockSize, true, offset, offset + blockSize, compress);
            index += 1;
        }
    }
    else
    {
        int64_t halfSize = blockSize / 2;
        int64_t offset = 0;
        int64_t remainderLength = size;
        for (; remainderLength > blockSize; remainderLength -= blockSize)
        {
            insertNewBlock(index, name, size, blockSize, true, offset, offset + blockSize, compress);
            index += 1;
            offset += blockSize;
        }

        if (remainderLength > halfSize)
        {
            insertNewBlock(index, name, size, blockSize, true, offset, offset + remainderLength, compress);
            index += 1;
        }
        else
        {
            index -= 1;
            updateOldBlock(index, name, size, blockSize, true, offset, offset + remainderLength, compress);
            index += 1;
        }
    }
}

}
}
