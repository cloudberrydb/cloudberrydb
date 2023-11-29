#include "fileSystem.h"
#include <iostream>

extern "C" {
#include "utils/elog.h"
#include "miscadmin.h"
}

#define GOPHER_BLOCK_SIZE (1024 * 1024 * 8)

namespace Datalake {
namespace Internal {

FileSystem::FileSystem()
{
	fs = NULL;
	file = NULL;
	closed = true;
}

FileSystem::~FileSystem()
{
	if (file != NULL)
	{
		closeFile();
		file = NULL;
	}
	if (fs != NULL)
	{
		gopherDestroyHandle();
		fs = NULL;
	}
}

bool FileSystem::checkCanceled(void)
{
	return InterruptPending;
}

int FileSystem::gopherCreateHandle(gopherConfig *conf)
{

	gopherUserCanceledCallBack(&checkCanceled);

	fs = gopherConnect(*conf);

	if (fs == NULL)
		throw Error("failed to connect to gopher: %s.", gopherGetLastError());

	return 0;
}

int FileSystem::openFile(const char *path, int flag)
{
	if (path == NULL)
	{
		return -1;
	}
	closed = false;
	filePath = path;
	std::string gopher_prefix = "";
	std::string prefix = path;
	int num = prefix.find_first_of("/");
	if (num == 0)
	{
		gopher_prefix.append(path);
	}
	else
	{
		gopher_prefix.append("/").append(path);
	}

	elog(DEBUG5, "External table gopherOpenFile path:%s flag:%d block_size:%d",
					gopher_prefix.c_str(), flag, GOPHER_BLOCK_SIZE);

	file = gopherOpenFile(fs, gopher_prefix.c_str(), flag, GOPHER_BLOCK_SIZE);
	if (file == NULL)
	{
		elog(ERROR, "gopher open \"%s\" failed. error message:%s.", path, gopherGetLastError());
		return -1;
	}
	return 0;
}

int FileSystem::write(void *buff, int64_t size)
{
	elog(DEBUG5, "External table gopherWrite path %s size %ld.", filePath.c_str(), size);

	if (size == 0)
	{
		return size;
	}
	int32_t writtenSize = gopherWrite(fs, file, buff, size);
	if (writtenSize == -1)
	{
		throw Error("failed to write: \"%s\" size %ld %s.", filePath.c_str(), size,
			gopherGetLastError());
	}
	return writtenSize;
}

int FileSystem::read(void *buff, int64_t size) {
	elog(DEBUG5, "External table gopherRead path %s size %ld.", filePath.c_str(), size);

	if (size == 0)
	{
		return size;
	}
	int readSize = gopherRead(fs, file, buff, size);
	if (readSize == -1)
	{
		throw Error("failed to read: \"%s\" size %ld %s.", filePath.c_str(), size,
			gopherGetLastError());
	}
	return readSize;
}

int FileSystem::seek(int64_t postion) {
	elog(DEBUG5, "External table gopherSeek path %s postion %ld.", filePath.c_str(), postion);

	int ret = gopherSeek(fs, file, postion);
	if (ret == -1)
	{
		throw Error("failed to seek: \"%s\" postion %ld %s.", filePath.c_str(), postion,
			gopherGetLastError());
	}
	return ret;
}

int FileSystem::getUfsId() {
	elog(DEBUG5, "External table gopherGetUfsId.");

	int ufsId = gopherGetUfsId(fs);
	if (ufsId < 0)
	{
		elog(WARNING, "get invalid gopher ufsId %d %s.", ufsId, gopherGetLastError());
		return -1;
	}
	return ufsId;
}

int FileSystem::closeFile() {
	elog(DEBUG5, "External table gopherCloseFile.");

	if (closed)
	{
		/* file maybe already closed */
		return 0;
	}
	closed = true;
	int ret = gopherCloseFile(fs, file, true);
	if (ret == 0)
	{
		file = NULL;
	}
	else
	{
		file = NULL;
		elog(ERROR, "gopherCloseFile failed path \"%s\" return value %d error message:%s.",
			filePath.c_str(), ret, gopherGetLastError());
	}
	return ret;
}

gopherFileInfo *FileSystem::listInfo(const char *path, int &count, int recursive) {
	int numEntries = 0;
	std::string gopher_prefix = "";
	std::string prefix = "";

	if (recursive)
	{
		if (path != NULL)
		{
			prefix = path;
			if (prefix.front() != '/')
			{
				gopher_prefix.append("/").append(prefix);
			}
			else
			{
				gopher_prefix.append(prefix);
			}
		}
	}
	else
	{
		gopher_prefix.append(path);
	}

	elog(DEBUG5, "External table gopherListDirectory path %s recursive %d.", gopher_prefix.c_str(), recursive);

	gopherFileInfo *res = gopherListDirectory(fs, gopher_prefix.c_str(), recursive, &numEntries);
	if (numEntries == -1)
		throw Error("failed to list directory: \"%s\" %s.", path, gopherGetLastError());

	if (res == NULL)
	{
		numEntries = 0;
	}

	if (numEntries == 0)
	{
		elog(LOG, "External table read empty folder : %s.", gopher_prefix.c_str());
	}

	count = numEntries;
	return res;
}

gopherFileInfo* FileSystem::getFileInfo(const char* path) {
	elog(DEBUG5, "External table gopherGetFileInfo path %s.", path);

	gopherFileInfo* info = gopherGetFileInfo(fs, path);
	if (info == NULL)
	{
		throw Error("gopherGetFileInfo failed, path \"%s\" error message %s.", path, gopherGetLastError());
	}
	return info;
}

int FileSystem::gopherDestroyHandle() {
	elog(DEBUG5, "External table gopherDisconnect.");

	int ret = gopherDisconnect(fs);
	if (ret == 0)
	{
		fs = NULL;
	}
	else
	{
		fs = NULL;
		throw Error("failed to disconnect to gopher: return value %d, error message %s.", ret, gopherGetLastError());
	}
	return ret;
}

void FileSystem::freeListInfo(gopherFileInfo *list, int count) {
	if (list == NULL)
	{
		return;
	}
	elog(DEBUG5, "External table gopherFreeFileInfo.");

	gopherFreeFileInfo(list, count);
}

}
}