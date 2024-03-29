#ifndef DATALAKE_PROVIDER_H
#define DATALAKE_PROVIDER_H

#include <map>
#include <vector>
#include <memory>
#include <iostream>
#include "src/common/fileSystemWrapper.h"

#define DATALAKE_EXPORT_NAME ("datalake")
#define PARQUET_WRITE_SUFFIX ("parquet")
#define AVRO_WRITE_SUFFIX ("avro")
#define ORC_WRITE_SUFFIX ("orc")
#define CSV_WRITE_SUFFIX ("csv")
#define TEXT_WRITE_SUFFIX ("txt")

typedef enum CompressType
{
	UNCOMPRESS = 0,
	LZJB,
	ZLIB,
	GZIP,
	SNAPPY,
	ZSTD,
	LZ4,
	BROTLI,
	ZIP,
	UNSUPPORTCOMPRESS
} CompressType;

class Provider {

public:

	virtual void createHandler(void* sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual int64_t read(void **recordBatch);

	virtual int64_t readWithBuffer(void* buffer, int64_t length);

	virtual int64_t write(const void* buf, int64_t length);

	virtual void destroyHandler();

	virtual CompressType getCompressType(char* type);

	virtual std::string generateWriteFileName(std::string writePrefix, std::string suffix, int segid);

};

std::shared_ptr<Provider> getProvider(const char *type, bool readFdw, bool vectorization);

#endif //PROVIDER_H
