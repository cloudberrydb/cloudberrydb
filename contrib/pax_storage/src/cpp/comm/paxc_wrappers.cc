#include "comm/paxc_wrappers.h"
#include "comm/cbdb_api.h"

#include <unistd.h>
#include <sys/stat.h>

#define PAX_MICROPARTITION_NAME_LENGTH 2048
#define PAX_MICROPARTITION_DIR_POSTFIX "_pax"

namespace paxc {
// pax file operation
static void DeletePaxDirectoryPathRecursive(
    const char *path, const char *toplevel_path, bool delete_topleveldir,
    void (*action)(const char *fname, bool isdir, int elevel),
    bool process_symlinks, int elevel);

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel);

// MakedirRecursive: function used to create directory recursively by a
// specified directory path. parameter path IN directory path. return void.
void MakedirRecursive(const char *path) {
  char dirpath[PAX_MICROPARTITION_NAME_LENGTH];
  unsigned int pathlen = strlen(path);
  struct stat st {};

  Assert(path != NULL && path[0] != '\0' &&
         pathlen < PAX_MICROPARTITION_NAME_LENGTH);

  for (unsigned int i = 0; i <= pathlen; i++) {
    if (path[i] == '/' || path[i] == '\0') {
      strncpy(dirpath, path, i + 1);
      dirpath[i + 1] = '\0';
      if (stat(dirpath, &st) != 0) {
        if (MakePGDirectory(dirpath) != 0)
          ereport(
              ERROR,
              (errcode_for_file_access(),
               errmsg("MakedirRecursive could not create directory \"%s\": %m",
                      dirpath)));
      }
    }
  }
}

// CopyFile: function used to copy all files from specified directory path to
// another specified directory. parameter srcsegpath IN source directory path.
// parameter dstsegpath IN destination directory path.
// parameter dst IN destination relfilenode information.
// return void.
void CopyFile(const char *srcsegpath, const char *dstsegpath) {
  char *buffer = NULL;
  int64 left;
  off_t offset;
  int dstflags;
  //  Note: here File type is defined in PG instead of pax::File class.
  ::File srcfile;
  ::File dstfile;

  Assert(srcsegpath != NULL && srcsegpath[0] != '\0');
  Assert(dstsegpath != NULL && dstsegpath[0] != '\0');

  // TODO(Tony): needs to adjust BLCKSZ for pax storage.
  buffer = reinterpret_cast<char *>(palloc0(BLCKSZ));

  // FIXME(Tony): need to verify if there exits fd leakage problem here.
  srcfile = PathNameOpenFile(srcsegpath, O_RDONLY | PG_BINARY);
  if (srcfile < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("CopyFile could not open file %s: %m", srcsegpath)));

  // TODO(Tony): need to understand if O_DIRECT flag could be optimzed for data
  // copying in PAX.
  dstflags = O_CREAT | O_WRONLY | O_EXCL | PG_BINARY;

  dstfile = PathNameOpenFile(dstsegpath, dstflags);
  if (dstfile < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("CopyFile could not create destination file %s: %m",
                           dstsegpath)));

  // TODO(Tony): here needs to implement exception handling for pg function call
  // such as FileDiskSize failure.
  left = FileDiskSize(srcfile);
  if (left < 0)
    ereport(ERROR, (errcode_for_file_access(),
                    errmsg("CopyFile could not seek to end of file %s: %m",
                           srcsegpath)));

  offset = 0;
  while (left > 0) {
    int len;
    CHECK_FOR_INTERRUPTS();
    len = Min(left, BLCKSZ);
    if (FileRead(srcfile, buffer, len, offset, WAIT_EVENT_DATA_FILE_READ) !=
        len)
      ereport(ERROR,
              (errcode_for_file_access(),
               errmsg("CopyFile could not read %d bytes from file \"%s\": %m",
                      len, srcsegpath)));

    if (FileWrite(dstfile, buffer, len, offset, WAIT_EVENT_DATA_FILE_WRITE) !=
        len)
      ereport(ERROR,
              (errcode_for_file_access(),
               errmsg("CopyFile could not write %d bytes to file \"%s\": %m",
                      len, dstsegpath)));

    offset += len;
    left -= len;
  }

  if (FileSync(dstfile, WAIT_EVENT_DATA_FILE_IMMEDIATE_SYNC) != 0)
    ereport(ERROR,
            (errcode_for_file_access(),
             errmsg("CopyFile could not fsync file \"%s\": %m", dstsegpath)));
  FileClose(srcfile);
  FileClose(dstfile);
  pfree(buffer);
}

// DeletePaxDirectoryPath: Delete a directory and everything in it, if it
// exists. parameter dirname IN directory to delete recursively. parameter
// reserve_topdir IN flag indicate if reserve top level directory.
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir) {
  struct stat statbuf {};

  if (stat(dirname, &statbuf) != 0) {
    // Silently ignore missing directory.
    if (errno == ENOENT)
      return;
    else
      ereport(
          ERROR,
          (errcode_for_file_access(),
           errmsg("Check PAX file directory failed, directory path: \"%s\": %m",
                  dirname)));
  }

  DeletePaxDirectoryPathRecursive(dirname, dirname, delete_topleveldir,
                                  UnlinkIfExistsFname, false, LOG);
}

// BuildPaxDirectoryPath: function used to build pax storage directory path
// following pg convension, for example base/{database_oid}/{blocks_relid}_pax.
// parameter rd_node IN relfilenode information.
// parameter rd_backend IN backend transaction id.
// return palloc'd pax storage directory path.
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend) {
  char *relpath = NULL;
  char *paxrelpath = NULL;
  relpath = relpathbackend(rd_node, rd_backend, MAIN_FORKNUM);
  Assert(relpath[0] != '\0');
  paxrelpath = psprintf("%s%s", relpath, PAX_MICROPARTITION_DIR_POSTFIX);
  pfree(relpath);
  return paxrelpath;
}

static void UnlinkIfExistsFname(const char *fname, bool isdir, int elevel) {
  if (isdir) {
    if (rmdir(fname) != 0 && errno != ENOENT)
      ereport(elevel, (errcode_for_file_access(),
                       errmsg("could not remove directory \"%s\": %m", fname)));
  } else {
    // Use PathNameDeleteTemporaryFile to report filesize
    PathNameDeleteTemporaryFile(fname, false);
  }
}

static void DeletePaxDirectoryPathRecursive(
    const char *path, const char *toplevel_path, bool delete_topleveldir,
    void (*action)(const char *fname, bool isdir, int elevel),
    bool process_symlinks, int elevel) {
  DIR *dir;
  struct dirent *de;
  dir = AllocateDir(path);

  while ((de = ReadDirExtended(dir, path, elevel)) != NULL) {
    char subpath[MAXPGPATH * 2];
    CHECK_FOR_INTERRUPTS();

    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;

    snprintf(subpath, sizeof(subpath), "%s/%s", path, de->d_name);

    switch (get_dirent_type(subpath, de, process_symlinks, elevel)) {
      case PGFILETYPE_REG:
        (*action)(subpath, false, elevel);
        break;
      case PGFILETYPE_DIR:
        DeletePaxDirectoryPathRecursive(
            subpath, toplevel_path, delete_topleveldir, action, false, elevel);
        break;
      default:
        break;
    }
  }

  // ignore any error here for delete
  FreeDir(dir);

  // skip deleting top level dir if delete_topleveldir is set to false.
  if (delete_topleveldir || strncmp(path, toplevel_path, strlen(path)) != 0) {
    //  it's important to fsync the destination directory itself as individual
    //  file fsyncs don't guarantee that the directory entry for the file is
    //  synced.  However, skip this if AllocateDir failed; the action function
    //  might not be robust against that.

    if (dir) (*action)(path, true, elevel);
  }
}

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid *opfamily, FmgrInfo *finfo, StrategyNumber strategynum)
{
  FmgrInfo dummy;
  HeapTuple tuple;
  Oid opclass;
  Oid oprid;
  RegProcedure opcode;
  bool isNull;

  opclass = GetDefaultOpClass(atttypid, BRIN_AM_OID);
  if (!OidIsValid(opclass))
    return false;

  *opfamily = get_opclass_family(opclass);
  tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(*opfamily),
                           ObjectIdGetDatum(atttypid),
                           ObjectIdGetDatum(subtype),
                           Int16GetDatum(strategynum));

  if (!HeapTupleIsValid(tuple))
    return false; // not found operator

  oprid = DatumGetObjectId(SysCacheGetAttr(AMOPSTRATEGY, tuple,
                                            Anum_pg_amop_amopopr, &isNull));
  ReleaseSysCache(tuple);
  Assert(!isNull && RegProcedureIsValid(oprid));

  opcode = get_opcode(oprid);
  if (!RegProcedureIsValid(opcode))
    return false;

  fmgr_info_cxt(opcode, finfo ? finfo : &dummy, CurrentMemoryContext);
  return true;
}

}  // namespace paxc
