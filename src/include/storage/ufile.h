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

struct UFile;

typedef struct FileAm
{
	void (*close) (struct UFile *file);
	int (*read) (struct UFile *file, char *buffer, int amount);
	int (*write) (struct UFile *file, char *buffer, int amount);
	int64_t (*size) (struct UFile *file);
	const char *(*name) (struct UFile *file);
	const char *(*getLastError) (void);
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
extern void UFileClose(UFile *file);

extern int UFileRead(UFile *file, char *buffer, int amount);
extern int UFileWrite(UFile *file, char *buffer, int amount);

extern off_t UFileSize(UFile *file);
extern const char *UFileName(UFile *file);

extern void UFileUnlink(Oid spcId, const char *fileName);
extern bool UFileExists(Oid spcId, const char *fileName);

extern const char *UFileGetLastError(UFile *file);

extern char *formatLocalFileName(RelFileNode *relFileNode, const char *fileName);

extern struct FileAm localFileAm;
extern struct FileAm *currentFileAm;

#endif //UFILE_H
