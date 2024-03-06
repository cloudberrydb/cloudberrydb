#pragma once
#include "comm/cbdb_api.h"

#include <string>

#include "catalog/pax_aux_table.h"
#include "storage/micro_partition_metadata.h"

#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME 1
#define ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT 2
#define ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE 3
#define ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS 4
#define ANUM_PG_PAX_BLOCK_TABLES_PTVISIMAPNAME 5
#define NATTS_PG_PAX_BLOCK_TABLES 5

namespace paxc {
void CPaxCreateMicroPartitionTable(Relation rel);

Oid FindAuxIndexOid(Oid aux_relid, Snapshot snapshot);

void InsertMicroPartitionPlaceHolder(Oid aux_relid, const char *blockname);
void UpdateVisimap(Oid aux_relid, const char *blockname,
                   const char *visimap_filename);
void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot,
                               const char *blockname);
// Scan aux table
// seqscan: MicroPartitionInfoIterator
// index scan
struct ScanAuxContext {
 public:
  void BeginSearchMicroPartition(Oid aux_relid, Oid aux_index_relid,
                                 Snapshot snapshot, LOCKMODE lockmode,
                                 const char *blockname);
  void BeginSearchMicroPartition(Oid aux_relid, Snapshot snapshot,
                                 LOCKMODE lockmode) {
    BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot, lockmode,
                              nullptr);
  }
  HeapTuple SearchMicroPartitionEntry();
  void EndSearchMicroPartition(LOCKMODE lockmode);

  Relation GetRelation() { return aux_rel_; }

 private:
  Relation aux_rel_ = nullptr;
  SysScanDesc scan_ = nullptr;
};

void PaxAuxRelationSetNewFilenode(Oid aux_relid);
bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot);
void FetchMicroPartitionAuxRow(Relation rel, Snapshot snapshot, const char *blockname,
                               void (*callback)(Datum *values, bool *isnull, void *arg),
															 void *arg);
}  // namespace paxc

namespace pax {
class CCPaxAuxTable final {
 public:
  CCPaxAuxTable() = delete;
  ~CCPaxAuxTable() = delete;

  static void PaxAuxRelationSetNewFilenode(Relation rel,
                                           const RelFileNode *newrnode,
                                           char persistence);

  static void PaxAuxRelationNontransactionalTruncate(Relation rel);

  static void PaxAuxRelationCopyData(Relation rel, const RelFileNode *newrnode,
                                     bool createnewpath = true);

  static void PaxAuxRelationCopyDataForCluster(Relation old_rel,
                                               Relation new_rel);

  static void PaxAuxRelationFileUnlink(RelFileNode node, BackendId backend,
                                       bool delete_topleveldir);
};

}  // namespace pax

namespace cbdb {

Oid GetPaxAuxRelid(Oid relid);

void InsertMicroPartitionPlaceHolder(Oid pax_relid,
                                     const std::string &blockname);
void InsertOrUpdateMicroPartitionEntry(const pax::WriteSummary &summary);

void UpdateVisimap(Oid aux_relid, const char *blockname,
                   const char *visimap_filename);

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot,
                               const std::string &blockname);
bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block,
                             Snapshot snapshot);

pax::MicroPartitionMetadata GetMicroPartitionMetadata(
    Relation rel, Snapshot snapshot, const std::string &blockname);

}  // namespace cbdb
