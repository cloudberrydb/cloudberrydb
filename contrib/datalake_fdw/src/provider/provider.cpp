#include <uuid/uuid.h>
#include <sys/time.h>
#include <sstream>
#include "src/provider/orc/read/orcReadRecordBatch.h"
#include "src/provider/parquet/read/parquetRead.h"
#include "src/provider/parquet/write/parquetWrite.h"
#include "src/provider/orc/read/orcRead.h"
#include "src/provider/orc/write/orcWriter.h"
#include "src/provider/archive/read/archiveRead.h"
#include "src/provider/archive/write/archiveWrite.h"
#include "src/provider/avro/read/avroRead.h"
#include "src/provider/avro/write/avroWrite.h"
#include "src/provider/iceberg/iceberg_read.h"
#include "src/provider/hudi/hudi_read.h"
#include "provider.h"
#include "src/common/util.h"

using Datalake::Internal::orcRead;
using Datalake::Internal::parquetRead;
using Datalake::Internal::avroRead;
using Datalake::Internal::archiveRead;
using Datalake::Internal::archiveWrite;
using Datalake::Internal::orcReadRecordBatch;
using Datalake::Internal::icebergRead;
using Datalake::Internal::hudiRead;


std::shared_ptr<Provider> getProvider(const char *type, bool readFdw, bool vectorization)
{
	if (readFdw)
	{
		if (strcmp(DATALAKE_OPTION_FORMAT_CSV, type) == 0)
		{
			return std::make_shared<archiveRead>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_TEXT, type) == 0)
		{
			return std::make_shared<archiveRead>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_ORC, type) == 0)
		{
			if (vectorization)
			{
				return std::make_shared<orcReadRecordBatch>();
			}
			else
			{
				return std::make_shared<orcRead>();
			}
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_PARQUET, type) == 0)
		{
			return std::make_shared<parquetRead>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_AVRO, type) == 0)
		{
			return std::make_shared<avroRead>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_ICEBERG, type) == 0)
		{
			return std::make_shared<icebergRead>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_HUDI, type) == 0)
		{
			return std::make_shared<hudiRead>();
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("unknow format \"%s\". "
					"datalake_fdw support read format text|csv|orc|parquet|avro.",
					type)));
		}
	}
	else
	{
		if (strcmp(DATALAKE_OPTION_FORMAT_CSV, type) == 0)
		{
			return std::make_shared<archiveWrite>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_TEXT, type) == 0)
		{
			return std::make_shared<archiveWrite>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_ORC, type) == 0)
		{
			return std::make_shared<orcWrite>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_PARQUET, type) == 0)
		{
			return std::make_shared<parquetWrite>();
		}
		else if (strcmp(DATALAKE_OPTION_FORMAT_AVRO, type) == 0)
		{
			return std::make_shared<avroWrite>();
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("unknow format \"%s\". "
					"datalake_fdw support write format text|csv|orc|parquet|avro.",
					type)));
		}
	}
	return NULL;
}

void Provider::createHandler(void* sstate)
{
	return;
}

int64_t Provider::read(void *values, void *nulls)
{
	return 0;
}

int64_t Provider::read(void **recordBatch)
{
	return 0;
}

int64_t Provider::readWithBuffer(void* buffer, int64_t length)
{
	return 0;
}

int64_t Provider::write(const void* buf, int64_t length)
{
	return 0;
}

void Provider::destroyHandler()
{
	return;
}

CompressType Provider::getCompressType(char* type)
{
	CompressType compresstype = UNCOMPRESS;
	if (type == NULL)
	{
		return compresstype;
	}

	if (strcmp(strConvertLow(type), "uncompress") == 0 ||
		strcmp(strConvertLow(type), "none") == 0)
	{
		compresstype = UNCOMPRESS;
	}
	else if (strcmp(strConvertLow(type), "zlib") == 0)
	{
		compresstype = ZLIB;
	}
	else if (strcmp(strConvertLow(type), "gzip") == 0)
	{
		compresstype = GZIP;
	}
	else if (strcmp(strConvertLow(type), "snappy") == 0)
	{
		compresstype = SNAPPY;
	}
	else if (strcmp(strConvertLow(type), "zstd") == 0)
	{
		compresstype = ZSTD;
	}
	else if (strcmp(strConvertLow(type), "lz4") == 0)
	{
		compresstype = LZ4;
	}
	else if (strcmp(strConvertLow(type), "brotli") == 0)
	{
		compresstype = BROTLI;
	}
	else if (strcmp(strConvertLow(type), "zip") == 0)
	{
		compresstype = ZIP;
	}
	else
	{
		elog(ERROR, "datalake unkonw compress type %s", type);
	}

	return compresstype;
}

std::string Provider::generateWriteFileName(std::string writePrefix, std::string suffix, int segid)
{
	uuid_t uuid;
    uuid_generate_time(uuid);
    char str[36] = {0};
    uuid_unparse(uuid, str);
    std::string uuid_str = str;

	time_t second = time(NULL);
	int64_t timestamp = static_cast<int64_t>(second);

	std::stringstream fileName;
	if (!writePrefix.empty())
	{
		fileName << writePrefix;
	}

	if (fileName.str().back() != '/')
	{
		fileName << "/";
	}
	fileName << timestamp << "/";
	fileName << uuid_str << "-" << segid;
	if (!suffix.empty())
	{
		fileName << "." << suffix;
	}
	return fileName.str();
}
