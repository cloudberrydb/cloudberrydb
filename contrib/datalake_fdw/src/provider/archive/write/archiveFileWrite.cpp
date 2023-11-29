#include <archive.h>
#include <archive_entry.h>
#include "archiveFileWrite.h"


namespace Datalake {
namespace Internal {


void archiveFileWrite::open(ossFileStream ossFile, std::string fileName, void *sstate, writeOption option)
{
	this->name = fileName;
	this->option = option;
	this->ossFile = ossFile;

	archive = archive_write_new();
	archive_entry =  archive_entry_new();

	openFile(ossFile, fileName.c_str(), O_CREAT);

	if (option.compression == UNCOMPRESS)
	{
		return;
	}

	if (option.compression == GZIP)
	{
		archive_write_add_filter_gzip(archive);
		archive_write_set_format_raw(archive);
	}
	else if (option.compression == ZIP)
	{
		archive_write_set_format_zip(archive);
		archive_write_set_options(archive, "zip:zip64");
	}

	if (archive_write_get_bytes_in_last_block(archive) == -1)
		archive_write_set_bytes_in_last_block(archive, 1);

	if (archive_write_open(archive, ossFile, NULL, write_call_back, NULL) != ARCHIVE_OK)
	{
		elog(ERROR, "Datalake Error, file %s archive_write_open error: %s",
			fileName.c_str(),
			archive_error_string(archive));
	}

	archive_entry_set_pathname(archive_entry, fileName.c_str());

	if (option.compression == GZIP)
	{
		archive_entry_set_filetype(archive_entry, AE_IFREG);
		archive_entry_set_perm(archive_entry, 0644);
	}
	else if (option.compression == ZIP)
	{
		archive_entry_unset_size(archive_entry);
		archive_entry_set_mode(archive_entry, AE_IFREG | 0621);
		archive_entry_set_mtime(archive_entry, time(NULL), 0);
	}

	if (archive_write_header(archive, archive_entry) != ARCHIVE_OK)
	{
		elog(ERROR, "Datalake Error, file %s archive_write_header error: %s",
			fileName.c_str(),
			archive_error_string(archive));
	}

}

int64_t archiveFileWrite::write(const void* buf, size_t length)
{
	int64_t n = 0;
	if (option.compression == UNCOMPRESS)
	{
		n = writeFile(ossFile, (void*)buf, length);
	}
	else
	{
		n = archive_write_data(archive, (void*)buf, length);
	}
	return n;
}


ssize_t archiveFileWrite::write_call_back(struct archive * arch, void *client_data, const void *buffer, size_t length)
{
	ossFileStream stream = (ossFileStream)client_data;
	int size = writeFile(stream, (void*)buffer, length);
	return size;
}

void archiveFileWrite::close()
{
	if (option.compression != UNCOMPRESS)
	{
		archive_entry_free(archive_entry);
		if (archive_write_close(archive) != ARCHIVE_OK)
		{
			elog(ERROR, "Datalake Error, file %s archive_write_close error: %s",
				name.c_str(),
				archive_error_string(archive));
		}

		if (archive_write_free(archive) != ARCHIVE_OK)
		{
			elog(ERROR, "Datalake Error, file %s archive_write_free error: %s",
				name.c_str(),
				archive_error_string(archive));
		}
	}

	closeFile(ossFile);
}

}
}