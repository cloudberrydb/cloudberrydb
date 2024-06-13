#include "comm/paxc_wrappers.h"

#include "comm/cbdb_api.h"

#include <sys/stat.h>
#include <unistd.h>

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
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend,
                            bool is_dfs_path) {
  char *relpath = NULL;
  char *paxrelpath = NULL;
  relpath = relpathbackend(rd_node, rd_backend, MAIN_FORKNUM);
  Assert(relpath[0] != '\0');
  if (is_dfs_path) {
    paxrelpath = psprintf("/%s%s/%d", relpath, PAX_MICROPARTITION_DIR_POSTFIX,
                          GpIdentity.segindex);
  } else {
    paxrelpath = psprintf("%s%s", relpath, PAX_MICROPARTITION_DIR_POSTFIX);
  }
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

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid *opfamily,
                               FmgrInfo *finfo, StrategyNumber strategynum) {
  FmgrInfo dummy;
  HeapTuple tuple;
  Oid opclass;
  Oid oprid;
  RegProcedure opcode;
  bool isNull;

  opclass = GetDefaultOpClass(atttypid, BRIN_AM_OID);
  if (!OidIsValid(opclass)) return false;

  *opfamily = get_opclass_family(opclass);
  tuple = SearchSysCache4(AMOPSTRATEGY, ObjectIdGetDatum(*opfamily),
                          ObjectIdGetDatum(atttypid), ObjectIdGetDatum(subtype),
                          Int16GetDatum(strategynum));

  if (!HeapTupleIsValid(tuple)) return false;  // not found operator

  oprid = DatumGetObjectId(
      SysCacheGetAttr(AMOPSTRATEGY, tuple, Anum_pg_amop_amopopr, &isNull));
  ReleaseSysCache(tuple);
  Assert(!isNull && RegProcedureIsValid(oprid));

  opcode = get_opcode(oprid);
  if (!RegProcedureIsValid(opcode)) return false;

  fmgr_info_cxt(opcode, finfo ? finfo : &dummy, CurrentMemoryContext);
  return true;
}

// can't use dfs-tablespace-ext directly, copy it
const char *GetDfsTablespaceServer(Oid spcId) {
  TableSpaceCacheEntry *spc = get_tablespace(spcId);

  if (!spc->opts) return NULL;

  if (spc->opts->serverOffset == 0) return NULL;

  return (const char *)spc->opts + spc->opts->serverOffset;
}

const char *GetDfsTablespacePath(Oid spcId) {
  TableSpaceCacheEntry *spc = get_tablespace(spcId);

  if (!spc->opts) return NULL;

  if (spc->opts->pathOffset == 0) return NULL;

  return (const char *)spc->opts + spc->opts->pathOffset;
}

FileAm *GetDfsTablespaceFileHandler(Oid spcId) {
  return ::GetTablespaceFileHandler(spcId);
}

bool IsDfsTablespaceById(Oid spcId) {
  if (spcId == InvalidOid) return false;

  TableSpaceCacheEntry *spc = get_tablespace(spcId);
  return spc->opts && (spc->opts->pathOffset != 0);
}

static void PaxFileDestroyPendingRelDelete(PendingRelDelete *reldelete) {
  PendingRelDeletePaxFile *filedelete;

  Assert(reldelete);
  filedelete = (PendingRelDeletePaxFile *)reldelete;

  pfree(filedelete->filenode.relativePath);
  pfree(filedelete);
}

static void PaxFileDoPendingRelDelete(PendingRelDelete *reldelete) {
  PendingRelDeletePaxFile *filedelete;
  Assert(reldelete);
  filedelete = (PendingRelDeletePaxFile *)reldelete;
  paxc::DeletePaxDirectoryPath(filedelete->filenode.relativePath, true);
}

static struct PendingRelDeleteAction pax_file_pending_rel_deletes_action = {
    .flags = PENDING_REL_DELETE_DEFAULT_FLAG,
    .destroy_pending_rel_delete = PaxFileDestroyPendingRelDelete,
    .do_pending_rel_delete = PaxFileDoPendingRelDelete};

void PaxAddPendingDelete(Relation rel, RelFileNode rn_node, bool atCommit) {
  // UFile
  bool is_dfs_tablespace =
      paxc::IsDfsTablespaceById(rel->rd_rel->reltablespace);
  char *relativePath =
      paxc::BuildPaxDirectoryPath(rn_node, rel->rd_backend, is_dfs_tablespace);
  if (is_dfs_tablespace) {
    UFileAddPendingDelete(rel, rel->rd_rel->reltablespace, relativePath,
                          atCommit);
  } else {
    // LocalFile
    PendingRelDeletePaxFile *pending;
    /* Add the relation to the list of stuff to delete at abort */
    pending = (PendingRelDeletePaxFile *)MemoryContextAlloc(
        TopMemoryContext, sizeof(PendingRelDeletePaxFile));
    pending->filenode.relkind = rel->rd_rel->relkind;
    pending->filenode.relativePath =
        MemoryContextStrdup(TopMemoryContext, relativePath);

    pending->reldelete.atCommit = atCommit; /* delete if abort */
    pending->reldelete.nestLevel = GetCurrentTransactionNestLevel();

    pending->reldelete.relnode.node = rn_node;
    pending->reldelete.relnode.isTempRelation =
        rel->rd_backend == TempRelBackendId;
    pending->reldelete.relnode.smgr_which = SMGR_INVALID;

    pending->reldelete.action = &pax_file_pending_rel_deletes_action;
    RegisterPendingDelete(&pending->reldelete);
  }
  pfree(relativePath);
}

}  // namespace paxc
