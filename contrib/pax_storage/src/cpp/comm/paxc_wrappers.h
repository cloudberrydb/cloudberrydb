#pragma once

#include "comm/cbdb_api.h"

namespace paxc {
// pax file operation, will refactor it later
void CopyFile(const char *srcsegpath, const char *dstsegpath);
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir);
void MakedirRecursive(const char *path);
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend);
bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid *opfamily, FmgrInfo *finfo, StrategyNumber strategynum);
}  // namespace paxc
