
#ifndef DATALAKE_PARQUETINPUTSTREAM_H
#define DATALAKE_PARQUETINPUTSTREAM_H
#include <parquet/io/interface_hdw.h>
#include <gopher/gopher.h>
#include "src/common/fileSystemWrapper.h"

namespace Datalake {
namespace Internal {
class gopherReadFileSystem : public ::parquet::RandomAccessFile {

public:
    gopherReadFileSystem(ossFileStream stream, std::string filePath, bool enableCache) : stream_(stream), filePath_(filePath), enableCache(enableCache) {
        checkMagic = true;
        closed_ = true;
    };

    ::parquet_arrow::Status Open();

    ::parquet_arrow::Result<int64_t> GetSize();

    ::parquet_arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out);

    ::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> ReadAt(int64_t position, int64_t nbytes);

    ::parquet_arrow::Result<::parquet_arrow::util::string_view> Peek(int64_t nbytes) {
        return ::parquet_arrow::Status::NotImplemented("Peek not implemented");
    }

    ::parquet_arrow::Result<std::shared_ptr<::parquet_arrow::Buffer>> Read(int64_t nbytes);

    ::parquet_arrow::Result<int64_t> Read(int64_t nbytes, void* out);

    ::parquet_arrow::Result<int64_t> Tell() const {
        return pos_;
    }

    ::parquet_arrow::Status Seek(int64_t position);

    ::parquet_arrow::Status Close();

    bool closed() const {
        return closed_;
    }

    bool checkFileIsParquet();

private:
    ossFileStream stream_;
    int64_t fileSize_;
    int64_t pos_;
    bool closed_;
    std::string filePath_;
    bool checkMagic;
	bool enableCache;
};

}
}

#endif