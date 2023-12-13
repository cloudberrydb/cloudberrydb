#ifndef UFS_API_H
#define UFS_API_H

#include "postgres.h"

struct UfsIoMethods;

typedef struct UfsFile
{
	struct UfsIoMethods *methods;
} UfsFile;

/*
 * Create or open a "regular file", please refer to the Gopher Doc
 * for what a regular file is.
 *
 * fileName: fileName should not contain path information. Inside
 * the function: for DFS tablespace, we will use the location field
 * of the DFS tablespace as the prefix of the path. for local fs
 * tablespace, we use tablespace oid and db oid as the prefix of
 * the path
 *
 */
extern UfsFile *UfsFileOpen(RelFileNode *relFileNode,
							const char *fileName,
							int fileFlags,
							char *errorMessage,
							int errorMessageSize);

/*
 * Create or open a "metadata file", please refer to the Gopher Doc
 * for what a metadata file is.
 *
 * fileName: fileName should not contain path information. Inside
 * the function: for DFS tablespace, we will use the location field
 * of the DFS tablespace as the prefix of the path. for local fs
 * tablespace, we use tablespace oid and db oid as the prefix of
 * the path
 *
 */
extern UfsFile *UfsFileOpenEx(RelFileNode *relFileNode,
							  const char *fileName,
							  int fileFlags,
							  char *errorMessage,
							  int errorMessageSize);
extern void UfsFileClose(UfsFile *file);

extern int UfsFilePread(UfsFile *file, char *buffer, int amount, off_t offset);
extern int UfsFilePwrite(UfsFile *file, char *buffer, int amount, off_t offset);

extern int UfsFileRead(UfsFile *file, char *buffer, int amount);
extern int UfsFileWrite(UfsFile *file, char *buffer, int amount);

extern off_t UfsFileSeek(UfsFile *file, off_t offset);
extern off_t UfsFileSize(UfsFile *file);
extern const char *UfsFileName(UfsFile *file);

extern void UfsFileUnlink(Oid spcId, const char *fileName);
extern void UfsFileUnlinkEx(Oid spcId, const char *fileName);

extern const char *UfsGetLastError(UfsFile *file);

extern char *UfsMakeFullPathName(RelFileNode *relFileNode, const char *fileName);

#endif  /* UFS_API_H */
