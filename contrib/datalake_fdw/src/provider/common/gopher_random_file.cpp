#include "gopher_random_file.h"
extern "C" {
#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/elog.h"
}

extern bool disableCacheFile;

#include "utils.h"


GopherRandomAccessFile::GopherRandomAccessFile(gopherFS gopherFilesystem, std::string filePath)
	: gopherFilesystem_(gopherFilesystem), filePath_(filePath)
{
	gopherFile_ = NULL;
	fileSize_ = -1;
	offset_ = 0;
	isClosed_ = true;
}

GopherRandomAccessFile::~GopherRandomAccessFile() 
{
	Close();
}

parquet_arrow::Status
GopherRandomAccessFile::Open()
{
	if (!isClosed_)
		return parquet_arrow::Status::OK();

	int flag = O_RDONLY;
	if (disableCacheFile)
		flag |= O_RDTHR;

	gopherFile_ = gopherOpenFile(gopherFilesystem_, filePath_.c_str(), flag, BLOCK_SIZE);
	if (gopherFile_ == NULL)
	{
		std::string message = "failed to open gopher file \"" + filePath_ + "\":" + gopherGetLastError();
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	if (gopherSeek(gopherFilesystem_, gopherFile_, 0) == -1)
	{
		std::string message = "failed to seek gopher file \"" + filePath_ + "\":" + gopherGetLastError();
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	isClosed_ = false;

	return ::parquet_arrow::Status::OK();
}

parquet_arrow::Result<int64_t>
GopherRandomAccessFile::GetSize()
{
	if (isClosed_)
	{
		RETURN_NOT_OK(Open());

		gopherFileInfo* fileInfo = gopherGetFileInfo(gopherFilesystem_, filePath_.c_str());
		if (fileInfo == NULL)
		{
			std::string message = "failed to get gopher file \"" + filePath_ + "\" info:" + gopherGetLastError();
			return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
		}
		fileSize_ = fileInfo->mLength;
		gopherFreeFileInfo(fileInfo, 1);
	}

	return fileSize_;
}

parquet_arrow::Result<int64_t>
GopherRandomAccessFile::ReadAt(int64_t position, int64_t nbytes, void *out)
{
	if (gopherSeek(gopherFilesystem_, gopherFile_, position) == -1)
	{
		std::string message = "failed to seek gopher file \"" + filePath_ + "\":" + gopherGetLastError();
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ = position;

	int64_t bytes = gopherRead(gopherFilesystem_, gopherFile_, out, nbytes);
	if (bytes == -1)
	{
		std::string message = "failed to read gopher file \"" + filePath_ + "\":" + gopherGetLastError();
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ += bytes;
	return bytes;
}

parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>>
GopherRandomAccessFile::ReadAt(int64_t position, int64_t nbytes)
{
	ARROW_ASSIGN_OR_RAISE(auto buf, parquet_arrow::AllocateResizableBuffer(nbytes));
	if (nbytes > 0)
	{
		ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, ReadAt(position, nbytes, buf->mutable_data()));
		RETURN_NOT_OK(buf->Resize(bytesRead));
	}

	return std::move(buf);
}

parquet_arrow::Result<int64_t>
GopherRandomAccessFile::Read(int64_t nbytes, void *out)
{
	int64_t bytes = gopherRead(gopherFilesystem_, gopherFile_, out, nbytes);
	if (bytes == -1)
	{
		std::string message = "failed to read gopher file \"" + filePath_ + "\":" + gopherGetLastError();
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}

	offset_ += bytes;
	return bytes;
}

parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>>
GopherRandomAccessFile::Read(int64_t nbytes)
{
	ARROW_ASSIGN_OR_RAISE(auto buf, parquet_arrow::AllocateResizableBuffer(nbytes));
	if (nbytes > 0)
	{
		ARROW_ASSIGN_OR_RAISE(int64_t bytesRead, Read(nbytes, buf->mutable_data()));
		RETURN_NOT_OK(buf->Resize(bytesRead));
	}

	return std::move(buf);
}

parquet_arrow::Status
GopherRandomAccessFile::Seek(int64_t position)
{
	if (gopherSeek(gopherFilesystem_, gopherFile_, position) == -1)
	{
		std::string message = "failed to seek gopher file \"" + filePath_ + "\":" + gopherGetLastError();
		return parquet_arrow::Status(parquet_arrow::StatusCode::IOError, message.c_str());
	}
 
	offset_ = position;
	return parquet_arrow::Status::OK();
}

parquet_arrow::Status
GopherRandomAccessFile::Close()
{
	if (isClosed_)
		return parquet_arrow::Status::OK();

	isClosed_ = true;
	gopherCloseFile(gopherFilesystem_, gopherFile_, true);
	return parquet_arrow::Status::OK();
}

parquet_arrow::Result<int64_t>
GopherRandomAccessFile::Tell() const
{
	return offset_;
}

bool
GopherRandomAccessFile::closed() const
{
	return isClosed_;
}
