#include "parquetInputStream.h"

namespace Datalake {
namespace Internal {

::parquet_arrow::Status gopherReadFileSystem::Open()
{
    closed_ = false;
	int flag = O_RDONLY|O_RDTHR;
	if (enableCache)
	{
		flag = O_RDONLY;
	}
    openFile(stream_, filePath_.c_str(), flag);
    gopherFileInfo* info = getFileInfo(stream_, filePath_.c_str());
    fileSize_ = info->mLength;
    freeListDir(stream_, info, 1);
    if (fileSize_ == 0)
    {
        checkMagic = false;
        return ::parquet_arrow::Status::OK();
    }
    const char* magic = "PAR1";
    char fileMagic[5] = {0};
    readFile(stream_, fileMagic, 4);
    if (strncmp(fileMagic, magic, 4) != 0)
    {
        checkMagic = false;
    }
    seekFile(stream_, 0);
    return ::parquet_arrow::Status::OK();
}

::parquet_arrow::Result<int64_t> gopherReadFileSystem::GetSize()
{
    return fileSize_;
}

::parquet_arrow::Result<int64_t> gopherReadFileSystem::ReadAt(int64_t position, int64_t nbytes, void* out)
{
    seekFile(stream_, position);
    pos_ = position;
    int64_t bytes = readFile(stream_, out, nbytes);
    pos_ += bytes;
    return bytes;
}

::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> gopherReadFileSystem::ReadAt(int64_t position, int64_t nbytes)
{
    std::shared_ptr<parquet::ResizableBuffer> buffer = parquet::AllocateBuffer(NULL, nbytes);
    seekFile(stream_, position);
    pos_ = position;
    int64_t bytes = readFile(stream_, (void*)(buffer->data()), nbytes);
    pos_ += bytes;
    return std::move(buffer);
}

::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> gopherReadFileSystem::Read(int64_t nbytes) {
    std::shared_ptr<parquet::ResizableBuffer> buffer = parquet::AllocateBuffer(NULL, nbytes);
    int64_t bytes = readFile(stream_, (void*)(buffer->data()), nbytes);
    pos_ += bytes;
    return std::move(buffer);
}

::parquet_arrow::Result<int64_t> gopherReadFileSystem::Read(int64_t nbytes, void* out) {
    int64_t bytes = readFile(stream_, out, nbytes);
    pos_ += bytes;
    return bytes;
}

::parquet_arrow::Status gopherReadFileSystem::Seek(int64_t position) {
    seekFile(stream_, position);
    pos_ = position;
    return ::parquet_arrow::Status::OK();
}

::parquet_arrow::Status gopherReadFileSystem::Close() {
    if (closed_)
    {
        // file maybe already closed
        return ::parquet_arrow::Status::OK();
    }
    closed_ = true;
    closeFile(stream_);
    return ::parquet_arrow::Status::OK();
}

bool gopherReadFileSystem::checkFileIsParquet() {
    return checkMagic;
}

}
}