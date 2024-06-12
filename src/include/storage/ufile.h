/*-------------------------------------------------------------------------
 *
 * Unified file abstraction and manipulation.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * src/include/storage/ufile.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef UFILE_H
#define UFILE_H

#include "storage/relfilenode.h"

#define UFILE_ERROR_SIZE	1024

struct UFile;

typedef struct FileAm
{
	struct UFile* (*open) (Oid spcId, const char *fileName, int fileFlags,
				char *errorMessage, int errorMessageSize);
	int (*close) (struct UFile *file);
	int (*sync) (struct UFile *file);
	int (*read) (struct UFile *file, char *buffer, int amount);
	int (*pread) (struct UFile *file, char *buffer, int amount, off_t offset);
	int (*write) (struct UFile *file, char *buffer, int amount);
	int (*pwrite) (struct UFile *file, char *buffer, int amount, off_t offset);
	int64_t (*size) (struct UFile *file);
	int (*unlink) (Oid spcId, const char *fileName);
	int (*rmdir) (Oid spcId, const char *dirName);
	char* (*formatPathName) (RelFileNode *relFileNode);
	bool (*ensurePath) (Oid spcId, const char *pathName);
	bool (*exists) (Oid spcId, const char *fileName);
	const char *(*name) (struct UFile *file);
	const char *(*getLastError) (void);
	void (*getConnection) (Oid spcId);
} FileAm;

typedef struct UFile
{
	FileAm *methods;
} UFile;

extern UFile *UFileOpen(Oid spcId,
						const char *fileName,
						int fileFlags,
						char *errorMessage,
						int errorMessageSize);
extern int UFileClose(UFile *file);
extern int UFileSync(UFile *fiLe);

extern int UFileRead(UFile *file, char *buffer, int amount);
extern int UFilePRead(UFile *file, char *buffer, int amount, off_t offset);
extern int UFileWrite(UFile *file, char *buffer, int amount);
extern int UFilePWrite(UFile *file, char *buffer, int amount, off_t offset);

extern int64_t UFileSize(UFile *file);
extern const char *UFileName(UFile *file);

extern int UFileUnlink(Oid spcId, const char *fileName);
extern int UFileRmdir(Oid spcId, const char *dirName);
extern char* UFileFormatPathName(RelFileNode *relFileNode);
extern bool UFileEnsurePath(Oid spcId, const char *pathName);
extern bool UFileExists(Oid spcId, const char *fileName);

extern const char *UFileGetLastError(UFile *file);
extern void forceCacheUFileResource(Oid id);

extern struct FileAm localFileAm;

#endif //UFILE_H
