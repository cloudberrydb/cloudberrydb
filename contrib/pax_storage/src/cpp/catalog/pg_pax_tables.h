#pragma once
#include "comm/cbdb_api.h"

#define NATTS_PG_PAX_TABLES 3
#define ANUM_PG_PAX_TABLES_RELID 1
#define ANUM_PG_PAX_TABLES_AUXRELID 2
#define ANUM_PG_PAX_TABLES_PARTITIONSPEC 3

namespace paxc {

void InsertPaxTablesEntry(Oid relid, Oid blocksrelid, Node *partitionspec);

void GetPaxTablesEntryAttributes(Oid relid, Oid *blocksrelid,
                                 Node **partitionspec);

void PaxInitializePartitionSpec(Relation paxrel, Node *part);

static inline Oid GetPaxAuxRelid(Oid pax_relid) {
  Oid aux_relid;
  GetPaxTablesEntryAttributes(pax_relid, &aux_relid, nullptr);
  return aux_relid;
}

}  // namespace paxc
