#include "postgres.h"

#include <unistd.h>
#include "catalog/pg_tablespace.h"
#include "commands/storagecmds.h"
#include "storage/fd.h"
#include "storage/relfilenode.h"
#include "storage/lockdefs.h"
#include "storage/ufile.h"
#include "utils/resowner.h"
#include "utils/spccache.h"
#include "utils/syscache.h"
#include "utils/wait_event.h"

#include "dfs_option.h"
#include "dfs_tablespace.h"
#include "remotefile.h"
#include "remotefile_connection.h"

#include <gopher/gopher.h>

static UFile *remoteFileOpen(Oid spcId, const char *fileName, int fileFlags,
               char *errorMessage, int errorMessageSize);
static int remoteFileClose(UFile *file);
static int remoteFileSync(UFile *file);
static int remoteFileRead(UFile *file, char *buffer, int amount);
static int remoteFilePRead(UFile *file, char *buffer, int amount, off_t offset);
static int remoteFileWrite(UFile *file, char *buffer, int amount);
static int remoteFilePWrite(UFile *file, char *buffer, int amount, off_t offset);
static off_t remoteFileSize(UFile *file);
static int remoteFileUnlink(Oid spcId, const char *fileName);
static int remoteFileRmdir(Oid spcId, const char *dirName);
static char *remoteFormatPathName(RelFileNode *relFileNode);
static bool remoteEnsurePath(Oid spcId, const char *pathName);
static bool remoteFileExists(Oid spcId, const char *fileName);
static const char *remoteFileName(UFile *file);
static const char *remoteGetLastError(void);
static void remoteGetConnection(Oid spcId);

static char remoteFileErrorStr[UFILE_ERROR_SIZE];

static struct FileAm remoteFileAm = {
    .open = remoteFileOpen,
    .close = remoteFileClose,
    .sync = remoteFileSync,
    .read = remoteFileRead,
    .pread = remoteFilePRead,
    .write = remoteFileWrite,
    .pwrite = remoteFilePWrite,
    .size = remoteFileSize,
    .unlink = remoteFileUnlink,
    .rmdir = remoteFileRmdir,
    .formatPathName = remoteFormatPathName,
    .ensurePath = remoteEnsurePath,
    .exists = remoteFileExists,
    .name = remoteFileName,
    .getLastError = remoteGetLastError,
    .getConnection = remoteGetConnection,
};

typedef struct RemoteFileHandle
{
    gopherFS fs;
    gopherFile file;
    ResourceOwner owner;
    struct RemoteFileHandle *next;
    struct RemoteFileHandle *prev;
} RemoteFileHandle;

typedef struct RemoteFileEx
{
    FileAm *methods;
    char *fileName;
    RemoteFileHandle *handle;
} RemoteFileEx;

static RemoteFileHandle *openRemoteHandles = NULL;
static bool resownerCallbackRegistered;

FileAm *
remote_file_handler(void)
{
    FileAm *fileAm = NULL;

    fileAm = &remoteFileAm;

    return fileAm;
}

static RemoteFileHandle *
createRemoteFileHandle(void)
{
    RemoteFileHandle *result;

    result = MemoryContextAlloc(TopMemoryContext, sizeof(RemoteFileHandle));

    result->fs = NULL;
    result->file = NULL;
    result->prev = NULL;
    result->next = openRemoteHandles;
    result->owner = CurrentResourceOwner;

    if (openRemoteHandles)
        openRemoteHandles->prev = result;

    openRemoteHandles = result;

    return result;
}

static void
destroyRemoteFileHandle(RemoteFileHandle *handle)
{
    if (handle->prev)
        handle->prev->next = handle->next;
    else
        openRemoteHandles = openRemoteHandles->next;

    if (handle->next)
        handle->next->prev = handle->prev;

    pfree(handle);
}

static void
remoteFileAbortCallback(ResourceReleasePhase phase,
                        bool isCommit,
                        bool isTopLevel,
                        void *arg)
{
    RemoteFileHandle *curr;
    RemoteFileHandle *next;

    if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
        return;

    next = openRemoteHandles;
    while (next)
    {
        curr = next;
        next = curr->next;

        if (curr->owner == CurrentResourceOwner)
        {
            if (isCommit)
                elog(LOG, "remoteFile reference leak: %p still referenced", curr);

            destroyRemoteFileHandle(curr);
        }
    }
}

static UFile *
remoteFileOpen(Oid spcId,
               const char *fileName,
               int fileFlags,
               char *errorMessage,
               int errorMessageSize)
{
    RemoteFileEx *result;
    gopherFS connection;
    gopherFile gopherFile;
    const char *server;
    const char *tableSpacePath;

    server = GetDfsTablespaceServer(spcId);
    tableSpacePath = GetDfsTablespacePath(spcId);
    connection = RemoteFileGetConnection(server, tableSpacePath);
    if (connection == NULL)
    {
        strlcpy(errorMessage, gopherGetLastError(), errorMessageSize);
        return NULL;
    }

    if (!resownerCallbackRegistered)
    {
        RegisterResourceReleaseCallback(remoteFileAbortCallback, NULL);
        resownerCallbackRegistered = true;
    }
    
    result = palloc(sizeof(RemoteFileEx));
    result->methods = &remoteFileAm;
    result->fileName = pstrdup(fileName);
    result->handle = createRemoteFileHandle();

    gopherFile = gopherOpenFile(connection, result->fileName, fileFlags, REMOTE_FILE_BLOCK_SIZE);
    if (gopherFile == NULL)
    {
        strlcpy(errorMessage, gopherGetLastError(), errorMessageSize);
        return NULL;
    }

    result->handle->fs = connection;
    result->handle->file = gopherFile;
    return (UFile *) result;
}

static int
remoteFileClose(UFile *file)
{
    int ret;
    RemoteFileEx *remoteFile = (RemoteFileEx *)file;
    ret = gopherCloseFile(remoteFile->handle->fs, remoteFile->handle->file, true);
    remoteFile->handle->file = NULL;
    destroyRemoteFileHandle(remoteFile->handle);

    if (ret < 0)
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());

    pfree(remoteFile->fileName);
    return ret;
}

static int
remoteFileSync(UFile *file)
{
    elog(WARNING, "remote file not support sync");
    return 0;
}

static int
remoteFileRead(UFile *file, char *buffer, int amount)
{
    int bytes;
    RemoteFileEx *remoteFile = (RemoteFileEx *) file;

    pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
    bytes = gopherRead(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
    pgstat_report_wait_end();

    if (bytes < 0)
    {
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
        return -1;
    }

    return bytes;
}

static int remoteFilePRead(UFile *file, char *buffer, int amount, off_t offset) {
    int rc;
    RemoteFileEx *remoteFile = (RemoteFileEx *)file;
    rc = gopherSeek(remoteFile->handle->fs, remoteFile->handle->file, offset);
    // The return value of gopherSeek seems to be unreasonably set. Compared
    // with the lseek implementation, lseek returns the resulting offset
    // location as measured in bytes from the beginning of the file, and
    // return -1 represents failure.
    if (rc < 0)
    {
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
        return rc;
    }

    pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
    rc = gopherRead(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
    pgstat_report_wait_end();

    if (rc < 0)
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
    return rc;
}

static int remoteFilePWrite(UFile *file, char *buffer, int amount, off_t offset) {
    snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", "remote file not support pwrite");
    return -1;
}

static int
remoteFileWrite(UFile *file, char *buffer, int amount)
{
    int bytes;
    RemoteFileEx *remoteFile = (RemoteFileEx *) file;

    pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_WRITE);
    bytes = gopherWrite(remoteFile->handle->fs, remoteFile->handle->file, buffer, amount);
    pgstat_report_wait_end();

    if (bytes < 0)
    {
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
    }

    return bytes;
}

static off_t
remoteFileSize(UFile *file)
{
    off_t result;
    RemoteFileEx *remoteFile = (RemoteFileEx *) file;

    gopherFileInfo *fileInfo = gopherGetFileInfo(remoteFile->handle->fs, remoteFile->fileName);
    if (fileInfo == NULL)
    {
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
        return -1;
    }

    result = fileInfo->mLength;
    gopherFreeFileInfo(fileInfo, 1);

    return result;
}

static int
remoteFileUnlink(Oid spcId, const char *fileName)
{
    int ret;
    const char *server;
    const char *tableSpacePath;
    gopherFS connection;

    server = GetDfsTablespaceServer(spcId);
    tableSpacePath = GetDfsTablespacePath(spcId);
    connection = RemoteFileGetConnection(server, tableSpacePath);
    if (connection == NULL) 
    {
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
        return -1;
    }

    ret = gopherPrefixDelete(connection, fileName);
    if (ret < 0)
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
    return ret;
}

static int
remoteFileRmdir(Oid spcId, const char *dirName)
{
    int ret;
    const char *server;
    const char *tableSpacePath;
    gopherFS connection;

    server = GetDfsTablespaceServer(spcId);
    tableSpacePath = GetDfsTablespacePath(spcId);
    connection = RemoteFileGetConnection(server, tableSpacePath);
    if (connection == NULL) 
    {
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
        return -1;
    }

    ret = gopherPrefixDelete(connection, dirName);
    if (ret < 0)
        snprintf(remoteFileErrorStr, UFILE_ERROR_SIZE, "%s", gopherGetLastError());
    return ret;
}

static char *
remoteFormatPathName(RelFileNode *relFileNode)
{
    char *remoteFileName;

    remoteFileName =
        psprintf("/%u/%u/"UINT64_FORMAT"_dirtable", relFileNode->spcNode, relFileNode->dbNode, relFileNode->relNode);;

    return remoteFileName;
}

bool
remoteEnsurePath(Oid spcId, const char *pathName)
{
    return true;
}

static bool
remoteFileExists(Oid spcId, const char *fileName)
{
    const char *server;
    const char *tableSpacePath;
    gopherFS connection;
    /* FIXME: gopher issue */
#define ENOENT_OSS 257
    server = GetDfsTablespaceServer(spcId);
    tableSpacePath = GetDfsTablespacePath(spcId);
    connection = RemoteFileGetConnection(server, tableSpacePath);
    if (connection == NULL)
    {
        elog(ERROR, "remoteFileExists error:%s", gopherGetLastError());
        return false;
    }

    bool exist = gopherExist(connection, fileName);

    if (exist)
        return true;

    if (errno != ENOENT && errno != ENOENT_OSS)
        ereport(ERROR,
                    (errcode_for_file_access(),
                     errmsg("unable to stat file \"%s\": %s", fileName, gopherGetLastError())));

    return false;
}

static const char *
remoteFileName(UFile *file)
{
    RemoteFileEx *remoteFile = (RemoteFileEx *) file;

    return remoteFile->fileName;
}

static const char *
remoteGetLastError(void)
{
    return remoteFileErrorStr;
}

static void
remoteGetConnection(Oid spcId)
{
    const char *server;
    const char *tableSpacePath;
    gopherFS connection;

    server = GetDfsTablespaceServer(spcId);
    tableSpacePath = GetDfsTablespacePath(spcId);
    connection = RemoteFileGetConnection(server, tableSpacePath);
    if (connection == NULL)
    {
        elog(ERROR, "remoteGetConnection error:%s", gopherGetLastError());
    }
}
