#ifndef DATALAKE_AVRO_OSS_INPUT_STREAM_H
#define DATALAKE_AVRO_OSS_INPUT_STREAM_H

#include <avro/Stream.hh>
#include "src/provider/provider.h"

class avroOssInputStream : public avro::SeekableInputStream
{
    const size_t bufferSize_;
    uint8_t *const buffer_;
    ossFileStream in_;
    size_t byteCount_;
    uint8_t *next_;
    size_t available_;
    std::string filePath_;
    int64_t fileSize_;
    int64_t pos_;

    bool next(const uint8_t **data, size_t *size) final
    {
        if (available_ == 0 && !fill())
        {
            return false;
        }
        *data = next_;
        *size = available_;
        next_ += available_;
        byteCount_ += available_;
        available_ = 0;
        return true;
    }

    void backup(size_t len) final
    {
        next_ -= len;
        available_ += len;
        byteCount_ -= len;
    }

    void skip(size_t len) final
    {
        while (len > 0)
        {
            if (available_ == 0)
            {
                byteCount_ += len;
                seekFile(in_, byteCount_);
                return;
            }
            size_t n = std::min(available_, len);
            available_ -= n;
            next_ += n;
            len -= n;
            byteCount_ += n;
        }
    }

    size_t byteCount() const final { return byteCount_; }

    bool fill()
    {
        size_t n = readFile(in_, buffer_, bufferSize_);
        if (n > 0)
        {
            next_ = buffer_;
            available_ = n;
            return true;
        }
        return false;
    }

    void seek(int64_t position) final
    {
        seekFile(in_, position);
        byteCount_ = position;
        available_ = 0;
    }

public:
    avroOssInputStream(ossFileStream in, std::string &filePath, size_t bufferSize, void * const buffer, bool shouldCache) :
                        bufferSize_(bufferSize),
                        buffer_((uint8_t *)buffer),
                        in_(in),
                        byteCount_(0),
                        next_(buffer_),
                        available_(0),
                        filePath_(filePath),
                        pos_(0)
    {
        int flag = O_RDONLY|O_RDTHR;
        if (shouldCache)
        {
            flag = O_RDONLY;
        }
        openFile(in_, filePath_.c_str(), flag);
        gopherFileInfo *info = getFileInfo(in_, filePath_.c_str());
        fileSize_ = info->mLength;
        freeListDir(in_, info, 1);
    }

    ~avroOssInputStream() override
    {
        closeFile(in_);
    }
};

typedef std::unique_ptr<avroOssInputStream> avroOssInputStreamPtr;

class BoundedInputStream : public avro::InputStream {
    avro::InputStream &in_;
    size_t limit_;

    bool next(const uint8_t **data, size_t *len) final
    {
        if (limit_ != 0 && in_.next(data, len))
        {
            if (*len > limit_)
            {
                in_.backup(*len - limit_);
                *len = limit_;
            }
            limit_ -= *len;
            return true;
        }
        return false;
    }

    void backup(size_t len) final
    {
        in_.backup(len);
        limit_ += len;
    }

    void skip(size_t len) final
    {
        if (len > limit_)
        {
            len = limit_;
        }
        in_.skip(len);
        limit_ -= len;
    }

    size_t byteCount() const final
    {
        return in_.byteCount();
    }

public:
    BoundedInputStream(InputStream &in, size_t limit) : in_(in), limit_(limit) {}
};

typedef std::unique_ptr<BoundedInputStream> BoundedInputStreamPtr;

#endif //DATALAKE_AVRO_OSS_INPUT_STREAM_H