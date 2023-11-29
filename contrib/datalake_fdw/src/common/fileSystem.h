#ifndef DATALAKE_FILESYSTEM_H
#define DATALAKE_FILESYSTEM_H

#include "gopher/gopher.h"
#include <exception>
#include <iostream>
#include <cstdarg>
#include <cstddef>

#define ERROR_STR_LEN 1024

namespace Datalake {
namespace Internal {
struct Error : std::exception
{
	char text[ERROR_STR_LEN];

	Error(char const* fmt, ...) __attribute__((format(printf,2,3)))
	{
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(text, sizeof text, fmt, ap);
		va_end(ap);
	}

	char const* what() const throw() { return text; }
};

class FileSystem {

public:
	FileSystem();

	~FileSystem();

	int gopherCreateHandle(gopherConfig *conf);

	int openFile(const char *path, int flag);

	int write(void *buff, int64_t size);

	int read(void *buff, int64_t size);

	int seek(int64_t postion);

	int closeFile();

	int getUfsId();

	gopherFileInfo* listInfo(const char *path, int &count, int recursive = 1);

	gopherFileInfo* getFileInfo(const char* path);

	void freeListInfo(gopherFileInfo *list, int count);

	// used internal for FileSystem.
	int gopherDestroyHandle();

private:
	static bool checkCanceled(void);
	gopherFS fs;
	gopherFile file;
	std::string filePath;
	bool closed;

};

}
}

#endif //HASHDATA_FILESYSTEM_H
