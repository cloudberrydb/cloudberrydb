/*-------------------------------------------------------------------------
 *
 * Unified file abstraction and manipulation.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 *
 *  * IDENTIFICATION
 *	  src/backend/storage/file/ufile.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_tablespace.h"
#include "cdb/cdbvars.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "storage/ufile.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/relfilenode.h"
#include "utils/elog.h"
#include "utils/wait_event.h"

#define UFILE_ERROR_SIZE	1024

typedef struct LocalFile
{
	FileAm * methods;
	File file;
	off_t offset;
} LocalFile;

static UFile *localFileOpen(Oid spcId, const char *fileName,
							int fileFlags, char *errorMessage, int errorMessageSize);
static void localFileClose(UFile *file);
static int	localFileRead(UFile *file, char *buffer, int amount);
static int	localFileWrite(UFile *file, char *buffer, int amount);
static off_t localFileSize(UFile *file);
static void localFileUnlink(const char *fileName);
static bool localFileExists(const char *fileName);
static const char *localFileName(UFile *file);
static const char *localGetLastError(void);

//static char *formatLocalFileName(RelFileNode *relFileNode, const char *fileName);

static char localFileErrorStr[UFILE_ERROR_SIZE];

struct FileAm localFileAm = {
	.close = localFileClose,
	.read = localFileRead,
	.write = localFileWrite,
	.size = localFileSize,
	.name = localFileName,
	.getLastError = localGetLastError,
};

//TODO
struct FileAm *currentFileAm = &localFileAm;

static UFile *
UFileOpenInternal(Oid spcId,
				  bool isNormalfile,
				  const char *fileName,
				  int fileFlags,
				  char *errorMessage,
				  int errorMessageSize)
{
//	bool isDfsTableSpace;

//	isDfsTableSpace = IsDfsTablespace(relFileNode->spcNode);
//	if (isDfsTableSpace)
//		return remoteFileOpen(relFileNode->spcNode,
//							  isNormalfile,
//							  fileName,
//							  fileFlags,
//							  errorMessage,
//							  errorMessageSize);

	return localFileOpen(spcId,
						 fileName,
						 fileFlags,
						 errorMessage,
						 errorMessageSize);
}

UFile *
UFileOpen(Oid spcId,
		  const char *fileName,
		  int fileFlags,
		  char *errorMessage,
		  int errorMessageSize)
{
	return UFileOpenInternal(spcId,
							 true,
							 fileName,
							 fileFlags,
							 errorMessage,
							 errorMessageSize);
}

static UFile *
localFileOpen(Oid spcId,
			  const char *fileName,
			  int fileFlags,
			  char *errorMessage,
			  int errorMessageSize)
{
	//char *filePath;
	LocalFile *result;
	File file;

	//filePath = formatLocalFileName(relFileNode, fileName);

	//file = PathNameOpenFile(filePath, fileFlags);
	file = PathNameOpenFile(fileName, fileFlags);
	if (file < 0)
	{
		snprintf(errorMessage, errorMessageSize, "%s", strerror(errno));
		return NULL;
	}

	result = palloc0(sizeof(LocalFile));
	result->methods = &localFileAm;
	result->file = file;
	result->offset = 0;

	//pfree(filePath);

	return (UFile *) result;
}

static void
localFileClose(UFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	FileClose(localFile->file);
}

static int
localFilePread(UFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	LocalFile *localFile = (LocalFile *) file;

	localFile->offset = offset;
	bytes = FileRead(localFile->file, buffer, amount, offset, WAIT_EVENT_DATA_FILE_READ);
	if (bytes < 0)
	{
		snprintf(localFileErrorStr, UFILE_ERROR_SIZE, "%s", strerror(errno));
		return -1;
	}

	localFile->offset += bytes;
	return bytes;
}

static int
localFileRead(UFile *file, char *buffer, int amount)
{
	LocalFile *localFile = (LocalFile *) file;

	return localFilePread(file, buffer, amount, localFile->offset);
}

static int
localFilePwrite(UFile *file, char *buffer, int amount, off_t offset)
{
	int bytes;
	LocalFile *localFile = (LocalFile *) file;

	localFile->offset = offset;
	bytes = FileWrite(localFile->file, buffer, amount, offset, WAIT_EVENT_DATA_FILE_WRITE);
	if (bytes < 0)
	{
		snprintf(localFileErrorStr, UFILE_ERROR_SIZE, "%s", strerror(errno));
		return -1;
	}

	localFile->offset += bytes;
	return bytes;
}

static int
localFileWrite(UFile *file, char *buffer, int amount)
{
	LocalFile *localFile = (LocalFile *) file;

	return localFilePwrite(file, buffer, amount, localFile->offset);
}

static off_t
localFileSize(UFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	return FileSize(localFile->file);
}

static bool
destory_local_file_directories(const char* directoryName)
{
	DIR		   *dirdesc;
	struct dirent *de;
	char	   *subfile;
	struct stat st;

	Assert(LWLockHeldByMe(DirectoryTableLock));

	elog(DEBUG5, "destory_local_file_directories for directory %s",
		 directoryName);

	dirdesc = AllocateDir(directoryName);
	if (dirdesc == NULL)
	{
		if (errno == ENOENT)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open directory \"%s\": %m",
							directoryName)));
			/* The symlink might still exist, so go try to remove it */
			return false;
		}
	}

	while ((de = ReadDir(dirdesc, directoryName)) != NULL)
	{
		if (strcmp(de->d_name, ".") == 0 ||
			strcmp(de->d_name, "..") == 0)
			continue;

		subfile = psprintf("%s/%s", directoryName, de->d_name);

		if (stat(subfile, &st) == 0 &&
			S_ISDIR(st.st_mode))
		{
			/* remove directory and file recursively */

			if (!destory_local_file_directories(subfile))
			{
				ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("directories for directory table \"%s\" could not be removed: %m",
			   						subfile),
							 errhint("You can remove the directories manually if necessary.")));

				FreeDir(dirdesc);
				pfree(subfile);

				return false;
			}
		}
		else
		{
			/* remove file */
			if (unlink(subfile) < 0)
				ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not remove files \"%s\": %m",
								   subfile)));
		}

		pfree(subfile);
	}

	FreeDir(dirdesc);

	/* remove directory */
	if (rmdir(directoryName) < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not remove directory \"%s\": %m",
						directoryName)));
	}

	return true;
}

static void
localFileUnlink(const char *fileName)
{
	struct stat st;

	LWLockAcquire(DirectoryTableLock, LW_EXCLUSIVE);
	if (stat(fileName, &st) == 0 &&
		S_ISDIR(st.st_mode))
	{
		/* remove directory and file recursively */
		if (!destory_local_file_directories(fileName))
			ereport(WARNING,
						(errcode_for_file_access(),
						 errmsg("directories for directory table \"%s\" could not be removed: %m",
							   fileName),
						 errhint("You can remove the directories manually if necessary.")));
		LWLockRelease(DirectoryTableLock);
	}
	else
	{
		LWLockRelease(DirectoryTableLock);
		/* remove file */
		if (unlink(fileName) < 0)
		{
			if (errno != ENOENT)
				ereport(WARNING,
							(errcode_for_file_access(),
							 errmsg("could not remove file \"%s\": %m", fileName)));
		}
	}

}

static bool
localFileExists(const char *fileName)
{
	struct stat fileStats;

	if (stat(fileName, &fileStats) != 0)
	{
		if (errno == ENOENT)
			return false;

		ereport(ERROR,
					(errcode_for_file_access(),
				 	 errmsg("unable to stat file \"%s\": %m", fileName)));
	}

	return true;
}

static const char *
localFileName(UFile *file)
{
	LocalFile *localFile = (LocalFile *) file;

	return FilePathName(localFile->file);
}

static const char *
localGetLastError(void)
{
	return localFileErrorStr;
}

void
UFileClose(UFile *file)
{
	file->methods->close(file);
	//TODO pfree move to close
	pfree(file);
}

int
UFileRead(UFile *file, char *buffer, int amount)
{
	return file->methods->read(file, buffer, amount);
}

int
UFileWrite(UFile *file, char *buffer, int amount)
{
	return file->methods->write(file, buffer, amount);
}

int64_t
UFileSize(UFile *file)
{
	return file->methods->size(file);
}

const char *
UFileName(UFile *file)
{
	return file->methods->name(file);
}


// TODO dfs
static void
UFileUnlinkInternal(Oid spcId, bool isNormalFile, const char *fileName)
{
	localFileUnlink(fileName);
}

void
UFileUnlink(Oid spcId, const char *fileName)
{
	return UFileUnlinkInternal(spcId, true, fileName);
}

bool
UFileExists(Oid spcId, const char *fileName)
{
//	bool isDfsTableSpace;
//
//	isDfsTableSpace = IsDfsTablespaceById(spcId);
//	if (isDfsTableSpace)
//	{
//		const char *server = GetDfsTablespaceServer(spcId);
//		const char *tableSpacePath = GetDfsTablespacePath(spcId);
//		gopherFS connection = UfsGetConnection(server, tableSpacePath);
//
//		return remoteFileExists(connection, fileName);
//	}

	return localFileExists(fileName);
}

const char *
UFileGetLastError(UFile *file)
{
	return file->methods->getLastError();
}

char *
formatLocalFileName(RelFileNode *relFileNode, const char *fileName)
{
	if (relFileNode->spcNode == DEFAULTTABLESPACE_OID)
		return psprintf("base/%u/%s", relFileNode->dbNode, fileName);
	else
		return psprintf("pg_tblspc/%u/%s/%u/"UINT64_FORMAT"_dirtable/%s",
						relFileNode->spcNode, GP_TABLESPACE_VERSION_DIRECTORY,
						relFileNode->dbNode, relFileNode->relNode, fileName);
}
