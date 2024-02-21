#ifndef GOPHER_RANDOM_FILE_H
#define GOPHER_RANDOM_FILE_H

#include <parquet/io/interface_hdw.h>
#include <gopher/gopher.h>

class GopherRandomAccessFile : public parquet::RandomAccessFile
{
public:
	GopherRandomAccessFile(gopherFS gopherFilesystem, std::string filePath);
	~GopherRandomAccessFile();

	parquet_arrow::Status Open();
	parquet_arrow::Result<int64_t> GetSize();
	parquet_arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void *out);
	parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes);
	parquet_arrow::Result<int64_t> Read(int64_t nbytes, void *out);
	parquet_arrow::Result<std::shared_ptr<parquet_arrow::Buffer>> Read(int64_t nbytes);
	parquet_arrow::Result<int64_t> Tell() const;
	parquet_arrow::Status Seek(int64_t position);
	parquet_arrow::Status Close();
	bool closed() const;

private:
	gopherFS gopherFilesystem_;
	gopherFile gopherFile_;
	std::string filePath_;
	int64_t fileSize_;
	int64_t offset_;
	bool isClosed_;
};

#endif // GOPHER_RANDOM_FILE_H
