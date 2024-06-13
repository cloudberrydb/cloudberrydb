#pragma once

#include "comm/cbdb_api.h"

namespace paxc {
// pax file operation, will refactor it later
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir);
void MakedirRecursive(const char *path);
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend,
                            bool is_dfs_path);
bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid *opfamily,
                               FmgrInfo *finfo, StrategyNumber strategynum);
const char *GetDfsTablespaceServer(Oid spcId);
const char *GetDfsTablespacePath(Oid spcId);
FileAm *GetDfsTablespaceFileHandler(Oid spcId);
bool IsDfsTablespaceById(Oid spcId);

typedef struct PaxFileNodePendingDelete {
  char relkind;
  char *relativePath;
} PaxFileNodePendingDelete;

typedef struct PendingRelDeletePaxFile {
  PendingRelDelete reldelete;        /* base pending delete */
  PaxFileNodePendingDelete filenode; /* relation that may need to be
                                      * deleted */
} PendingRelDeletePaxFile;

void PaxAddPendingDelete(Relation rel, RelFileNode rn_node, bool atCommit);

}  // namespace paxc
