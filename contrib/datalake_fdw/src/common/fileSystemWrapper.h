#ifndef DATALAKE_FILESYSTEMWRAPPER_H
#define DATALAKE_FILESYSTEMWRAPPER_H
#include <gopher/gopher.h>

struct ossInternalFileStream;

typedef struct ossInternalFileStream *ossFileStream;

#ifdef __cplusplus
extern "C" {
#endif

gopherConfig* createGopherConfig(void *opt);

void freeGopherConfig(gopherConfig* conf);

ossFileStream createFileSystem(gopherConfig *conf);

int openFile(ossFileStream file, const char *path, int flag);

int writeFile(ossFileStream file, void *buff, int64_t size);

int readFile(ossFileStream file, void *buff, int64_t size);

int seekFile(ossFileStream file, int64_t postion);

int getUfsId(ossFileStream file);

int closeFile(ossFileStream file);

gopherFileInfo* listDir(ossFileStream file, const char *path, int *count, int recursive);

gopherFileInfo* getFileInfo(ossFileStream file, const char* path);

void freeListDir(ossFileStream file, gopherFileInfo *list, int count);

int gopherDestroyHandle(ossFileStream file);

void destroyFileSystem(ossFileStream file);
#ifdef __cplusplus
}
#endif


#endif
