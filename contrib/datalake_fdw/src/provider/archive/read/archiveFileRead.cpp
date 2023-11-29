#include <archive.h>
#include <archive_entry.h>
#include "archiveFileRead.h"

namespace Datalake {
namespace Internal {


void archiveFileRead::open(ossFileStream ossFile, std::string fileName, readOption options)
{
	openFile(ossFile, fileName.c_str(), setStreamFlag(options));

	archive = archive_read_new();
	archive_read_support_filter_all(archive);
	setArchiveReadSupport(fileName.c_str());
	this->fileName = fileName;

	int ret = archive_read_open(archive, ossFile, NULL, read_call_back, NULL);

	if (ret != ARCHIVE_OK)
	{
		elog(ERROR, "Datalake Error, file %s archive_read_open error: %s",
			fileName.c_str(), 
			archive_error_string(archive));
	}

	ret = archive_read_next_header(archive, &archive_entry);
	if (ret != ARCHIVE_OK)
	{
		elog(ERROR, "Datalake Error, file %s archive_read_next_header error: %s",
			fileName.c_str(), 
			archive_error_string(archive));
	}
	state = FILE_OPEN;

}

void archiveFileRead::setArchiveReadSupport(const char* fileName)
{
	const char* format_zip = ".zip";
    const char* format_targz = ".tar.gz";
    const char* format_tarbz2 = ".tar.bz2";

	int len = strlen(fileName);
	if ((len > 4 && strncmp(fileName + len - 4, format_zip, 4) == 0) ||
		(len > 7 && strncmp(fileName + len - 7, format_targz, 7) == 0) ||
		(len > 8 && strncmp(fileName + len - 8, format_tarbz2, 8) == 0))
	{
		archive_read_support_format_all(archive);
	}
	else
	{
		archive_read_support_format_raw(archive);
	}
}

ssize_t archiveFileRead::read_call_back(struct archive *a, void *client_data, const void **buffer)
{
	CHECK_FOR_INTERRUPTS();

	ossFileStream ossFile = (ossFileStream)client_data;
	int size = readFile(ossFile, archiveReadBuff, ARCHIEVE_READ_BUFFER_SIZE);
	*buffer = archiveReadBuff;
	return size;
}

int64_t archiveFileRead::read(void* buffer, size_t length)
{
	int64_t size = archive_read_data(archive, buffer, length);
	return size;
}

void archiveFileRead::close()
{
	state = FILE_CLOSE;
	if (archive)
	{
		if (ARCHIVE_OK != archive_read_close(archive))
		{
			elog(ERROR, "Datalake Error, file %s archive_read_close error: %s",
				fileName.c_str(), 
				archive_error_string(archive));
		}

		if (ARCHIVE_OK != archive_read_free(archive))
		{
			elog(ERROR, "Datalake Error, file %s archive_read_free error: %s",
				fileName.c_str(), 
				archive_error_string(archive));
		}

		archive = NULL;
	}
}

}
}