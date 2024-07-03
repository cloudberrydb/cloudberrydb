#include "textFileSimpleRead.h"

// extern "C"
// {
// #include "src/gpossext.h"
// }

namespace Datalake {
namespace Internal {


textFileSimpleRead::textFileSimpleRead() {
	state = FILE_UNDEF; 
}

textFileSimpleRead::~textFileSimpleRead() {

}

void textFileSimpleRead::open(ossFileStream ossFile, std::string fileName, readOption options) {
	this->stream = ossFile;
	this->fileName = fileName;
	state = FILE_OPEN;
	int ret = openFile(ossFile, fileName.c_str(), setStreamFlag(options));
	if (ret < 0) {
		elog(ERROR, "Datalake Error, open file %s failed! detail %s.", 
			fileName.c_str(), gopherGetLastError());
	}
}

int64_t textFileSimpleRead::read(void* buffer, size_t length) {
	int size = readFile(stream, buffer, length);
	if (size < 0) {
		elog(ERROR, "Datalake Error, read %lu in file %s failed! detail %s.", 
			length, fileName.c_str(), gopherGetLastError());
	}
	return size;
}

void textFileSimpleRead::close() {
	if (state == FILE_OPEN) {
		closeFile(stream);
	}
	state = FILE_CLOSE;
}

int64_t textFileSimpleRead::seek(int64_t offset) {
    int res = seekFile(stream, offset);
	if (res < 0) {
		elog(ERROR, "Datalake Error, seek %ld in file %s failed! detail %s.", 
			offset, fileName.c_str(), gopherGetLastError());
	}
	return res;
}


}
}