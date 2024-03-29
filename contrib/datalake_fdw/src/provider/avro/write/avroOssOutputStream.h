#ifndef DATALAKE_AVRO_OSS_OUTPUT_STREAM_H
#define DATALAKE_AVRO_OSS_OUTPUT_STREAM_H

#include <avro/Stream.hh>
#include "src/common/fileSystemWrapper.h"


class avroOssOutputStream : public avro::OutputStream
{
    std::string filePath_;
    size_t bufferSize_;
    uint8_t *const buffer_;
    ossFileStream out_;
    uint8_t *next_;
    size_t available_;
    size_t byteCount_;

    // Invariant: byteCount_ == bytesWritten + bufferSize_ - available_;
    bool next(uint8_t **data, size_t *len) final
    {
        if (available_ == 0)
        {
            flush();
        }
        *data = next_;
        *len = available_;
        next_ += available_;
        byteCount_ += available_;
        available_ = 0;
        return true;
    }

    void backup(size_t len) final
    {
        available_ += len;
        next_ -= len;
        byteCount_ -= len;
    }

    uint64_t byteCount() const final
    {
        return byteCount_;
    }

    void flush() final
    {
        writeFile(out_, buffer_, bufferSize_ - available_);
        next_ = buffer_;
        available_ = bufferSize_;
    }

public:
    avroOssOutputStream(ossFileStream out,
                        const std::string &name,
                        size_t bufferSize) :
                        filePath_(name),
                        bufferSize_(bufferSize),
                        buffer_(new uint8_t[bufferSize]),
                        out_(out),
                        next_(buffer_),
                        available_(bufferSize_),
                        byteCount_(0)
    {
        openFile(out_, filePath_.c_str(), O_WRONLY);
    }

    ~avroOssOutputStream() override
    {
        closeFile(out_);
        delete[] buffer_;
    }
};

typedef std::unique_ptr<avroOssOutputStream> avroOssOutputStreamPtr;

#endif // DATALAKE_AVRO_OSS_OUTPUT_STREAM_H