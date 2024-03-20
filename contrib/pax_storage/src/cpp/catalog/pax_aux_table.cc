#include "catalog/pax_aux_table.h"

#include "comm/cbdb_api.h"

#include <uuid/uuid.h>
#include <utility>

#include "catalog/pax_fastsequence.h"
#include "catalog/pg_pax_tables.h"
#include "comm/cbdb_wrappers.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/micro_partition_metadata.h"

namespace paxc {

static inline void InsertTuple(Relation rel, Datum *values, bool *nulls) {
  HeapTuple tuple;
  tuple = heap_form_tuple(RelationGetDescr(rel), values, nulls);
  CatalogTupleInsert(rel, tuple);
}

static inline void InsertTuple(Oid relid, Datum *values, bool *nulls) {
  Relation rel;

  rel = table_open(relid, RowExclusiveLock);
  InsertTuple(rel, values, nulls);
  table_close(rel, NoLock);
}

static void CPaxTransactionalTruncateTable(Oid aux_relid) {
  Relation aux_rel;
  Assert(OidIsValid(aux_relid));

  // truncate already exist pax block auxiliary table.
  aux_rel = relation_open(aux_relid, AccessExclusiveLock);

  /*TODO1 pending-delete operation should be applied here. */
  RelationSetNewRelfilenode(aux_rel, aux_rel->rd_rel->relpersistence);
  relation_close(aux_rel, NoLock);
}

// * non transactional truncate table case:
// 1. create table inside transactional block, and then truncate table inside
// transactional block.
// 2.create table outside transactional block, insert data
// and truncate table inside transactional block.
static void CPaxNontransactionalTruncateTable(Relation rel) {
  Relation aux_rel;
  Oid aux_relid;

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(rel));
  Assert(OidIsValid(aux_relid));

  aux_rel = relation_open(aux_relid, AccessExclusiveLock);
  heap_truncate_one_rel(aux_rel);
  relation_close(aux_rel, NoLock);

  paxc::CPaxInitializeFastSequenceEntry(RelationGetRelid(rel), FASTSEQUENCE_INIT_TYPE_INPLACE);
}

void CPaxCreateMicroPartitionTable(Relation rel) {
  Relation pg_class_desc;
  char aux_relname[32];
  Oid relid;
  Oid aux_relid;
  Oid aux_namespace_id;
  Oid pax_relid;
  TupleDesc tupdesc;

  pg_class_desc = table_open(RelationRelationId, RowExclusiveLock);
  pax_relid = RelationGetRelid(rel);

  // 1. create blocks table.
  snprintf(aux_relname, sizeof(aux_relname), "pg_pax_blocks_%u", pax_relid);
  aux_namespace_id = PG_EXTAUX_NAMESPACE;
  aux_relid = GetNewOidForRelation(pg_class_desc, ClassOidIndexId,
                                   Anum_pg_class_oid,  // new line
                                   aux_relname, aux_namespace_id);
  tupdesc = CreateTemplateTupleDesc(NATTS_PG_PAX_BLOCK_TABLES);
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME,
                     "ptblockname", NAMEOID, -1, 0);
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT,
                     "pttupcount", INT4OID, -1, 0);
  // TODO(chenhongjie): uncompressed and compressed ptblocksize are needed.
  TupleDescInitEntry(tupdesc, (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE,
                     "ptblocksize", INT4OID, -1, 0);
  TupleDescInitEntry(tupdesc,
                     (AttrNumber)ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS,
                     "ptstatistics", PAX_AUX_STATS_TYPE_OID, -1, 0);
  {
    // Add constraints for the aux table
    auto attr = TupleDescAttr(tupdesc, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1);
    attr->attnotnull = true;
  }
  relid = heap_create_with_catalog(
      aux_relname, aux_namespace_id, InvalidOid, aux_relid, InvalidOid,
      InvalidOid, rel->rd_rel->relowner, HEAP_TABLE_AM_OID, tupdesc, NIL,
      RELKIND_RELATION, RELPERSISTENCE_PERMANENT, rel->rd_rel->relisshared,
      RelationIsMapped(rel), ONCOMMIT_NOOP, NULL, /* GP Policy */
      (Datum)0, false,                            /* use _user_acl */
      true, true, InvalidOid, NULL,               /* typeaddress */
      false /* valid_opts */);
  Assert(relid == aux_relid);
  table_close(pg_class_desc, NoLock);

  NewRelationCreateToastTable(relid, (Datum)0);

  // 2. insert entry into pg_pax_tables.
  ::paxc::InsertPaxTablesEntry(pax_relid, aux_relid, NULL);

  // 3. record pg_depend, pg_pax_blocks_<xxx> depends relation.
  {
    ObjectAddress base;
    ObjectAddress aux;
    base.classId = RelationRelationId;
    base.objectId = pax_relid;
    base.objectSubId = 0;
    aux.classId = RelationRelationId;
    aux.objectId = aux_relid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);

    // pg_pax_tables single row depend
    base.classId = RelationRelationId;
    base.objectId = pax_relid;
    base.objectSubId = 0;
    aux.classId = PAX_TABLES_RELATION_ID;
    aux.objectId = pax_relid;
    aux.objectSubId = 0;
    recordDependencyOn(&aux, &base, DEPENDENCY_INTERNAL);
  }
  CommandCounterIncrement();

  // 4. create index on ptblockname dynamically, the index name should be pg_paxaux.pg_pax_blocks_index_xxx.
  {
    char aux_index_name[NAMEDATALEN];
    IndexInfo *indexInfo;
    List *indexColNames;
    Relation aux_rel;
    int16 coloptions[1];
    Oid classObjectId[1];
    Oid collationObjectId[1];

    snprintf(aux_index_name, sizeof(aux_index_name), "%s_idx", aux_relname);

    indexInfo = makeNode(IndexInfo);
    indexInfo->ii_NumIndexAttrs = 1;
    indexInfo->ii_NumIndexKeyAttrs = 1;
    indexInfo->ii_IndexAttrNumbers[0] = ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME;
    indexInfo->ii_Expressions = NIL;
    indexInfo->ii_ExpressionsState = NIL;
    indexInfo->ii_Predicate = NIL;
    indexInfo->ii_PredicateState = NULL;
    indexInfo->ii_Unique = true;
    indexInfo->ii_ReadyForInserts = true;
    indexInfo->ii_Concurrent = false;
    indexInfo->ii_Am = BTREE_AM_OID;
    indexInfo->ii_Context = CurrentMemoryContext;

    collationObjectId[0] = C_COLLATION_OID;
    classObjectId[0] = GetDefaultOpClass(NAMEOID, BTREE_AM_OID);
    coloptions[0] = 0;

    auto attr = TupleDescAttr(tupdesc, ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1);
    indexColNames = list_make1(NameStr(attr->attname));

    // ShareLock is not really needed here, but take it anyway.
    aux_rel = table_open(aux_relid, ShareLock);

    index_create(aux_rel,
                  aux_index_name,
                  InvalidOid,
                  InvalidOid,
                  InvalidOid,
                  InvalidOid,
                  indexInfo,
                  indexColNames,
                  BTREE_AM_OID,
                  // The tablespace in aux index should follow aux table
                  aux_rel->rd_rel->reltablespace,
                  collationObjectId, classObjectId, coloptions, (Datum) 0,
                  INDEX_CREATE_IS_PRIMARY, 0, true, true, NULL);

    // Unlock target table -- no one can see it
    table_close(aux_rel, ShareLock);

    // Unlock the index -- no one can see it anyway
    //UnlockRelationOid(paxauxiliary_idxid, AccessExclusiveLock);

    CommandCounterIncrement();
  }

}

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot,
                               const char *blockname) {
  ScanAuxContext context;
  HeapTuple tuple;
  Oid aux_relid;

  aux_relid = ::paxc::GetPaxAuxRelid(pax_relid);

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot, RowExclusiveLock, blockname);
  tuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(tuple))
    elog(ERROR, "delete micro partition \"%s\" failed for relation(%u)", blockname, pax_relid);

  Assert(context.GetRelation());
  CatalogTupleDelete(context.GetRelation(), &tuple->t_self);

  context.EndSearchMicroPartition(NoLock);
}

void InsertMicroPartitionPlaceHolder(Oid aux_relid, const char *blockname) {
  NameData ptblockname;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool nulls[NATTS_PG_PAX_BLOCK_TABLES];

  Assert(blockname && strlen(blockname) < NAMEDATALEN);
  namestrcpy(&ptblockname, blockname);

  values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = NameGetDatum(&ptblockname);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = false;

  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = true;
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = true;
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = true;

  InsertTuple(aux_relid, values, nulls);
  CommandCounterIncrement();
}

void InsertOrUpdateMicroPartitionPlaceHolder(Oid aux_relid,
                                      const char *blockname,
                                      int num_tuples, int file_size,
                                      const ::pax::stats::MicroPartitionStatisticsInfo &mp_stats) {
  int stats_length = mp_stats.ByteSize();
  uint32 len = VARHDRSZ + stats_length;
  void *output;

  NameData ptblockname;
  Datum values[NATTS_PG_PAX_BLOCK_TABLES];
  bool nulls[NATTS_PG_PAX_BLOCK_TABLES];

  output = palloc(len);
  SET_VARSIZE(output, len);
  mp_stats.SerializeToArray(VARDATA(output), stats_length);

  Assert(blockname);
  namestrcpy(&ptblockname, blockname);

  values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = NameGetDatum(&ptblockname);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME - 1] = false;

  values[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = Int32GetDatum(num_tuples);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTTUPCOUNT - 1] = false;
  values[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = Int32GetDatum(file_size);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKSIZE - 1] = false;

  // Serialize catalog statitics information into PG bytea format and saved in
  // aux table ptstatitics column.
  values[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = PointerGetDatum(output);
  nulls[ANUM_PG_PAX_BLOCK_TABLES_PTSTATISITICS - 1] = false;

  ScanAuxContext context;
  context.BeginSearchMicroPartition(aux_relid, InvalidOid, NULL, RowExclusiveLock, blockname);
  auto aux_rel = context.GetRelation();
  auto oldtuple = context.SearchMicroPartitionEntry();
  if (!HeapTupleIsValid(oldtuple))
    elog(ERROR, "micro partition doesn't exist before inserting tuples");

  if (num_tuples > 0) {
    auto newtuple = heap_form_tuple(RelationGetDescr(aux_rel), values, nulls);

    newtuple->t_data->t_ctid = oldtuple->t_data->t_ctid;
    newtuple->t_self = oldtuple->t_self;
    newtuple->t_tableOid = oldtuple->t_tableOid;
    CatalogTupleUpdate(aux_rel, &newtuple->t_self, newtuple);
    heap_freetuple(newtuple);
  } else {
    CatalogTupleDelete(aux_rel, &oldtuple->t_self);
  }
  context.EndSearchMicroPartition(NoLock);

  pfree(output);

  CommandCounterIncrement();
}

Oid FindAuxIndexOid(Oid aux_relid, Snapshot snapshot) {
  ScanKeyData scankey[1];
  Relation indrel;
  SysScanDesc scan;
  HeapTuple tuple;
  Oid index_oid;
  int index_count = 0;

  ScanKeyInit(&scankey[0], Anum_pg_index_indrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(aux_relid));
  indrel = table_open(IndexRelationId, AccessShareLock);
  scan = systable_beginscan(indrel, IndexIndrelidIndexId, true, snapshot, 1, scankey);

  index_oid = InvalidOid;
  while (HeapTupleIsValid(tuple = systable_getnext(scan))) {
    auto index = (Form_pg_index) GETSTRUCT(tuple);
    index_count++;
    if (!index->indislive || !index->indisvalid) continue;
    index_oid = index->indexrelid;
  }
  systable_endscan(scan);
  table_close(indrel, NoLock);

  if (index_count != 1 || !OidIsValid(index_oid))
    elog(ERROR, "unexpected number of index of aux table: %d", index_count);

  return index_oid;
}

static inline Oid GetAuxIndexOid(Oid aux_relid, Oid *aux_index_relid, Snapshot snapshot) {
  if (aux_index_relid) {
    if (OidIsValid(*aux_index_relid))
      return *aux_index_relid;
    else
      return *aux_index_relid = FindAuxIndexOid(aux_relid, snapshot);
  } else {
    return FindAuxIndexOid(aux_relid, snapshot);
  }
}

void ScanAuxContext::BeginSearchMicroPartition(Oid aux_relid, Oid aux_index_relid, Snapshot snapshot, LOCKMODE lockmode, const char *blockname) {
  Assert(aux_relid);
  if (!OidIsValid(aux_index_relid) && blockname)
    aux_index_relid = FindAuxIndexOid(aux_relid, snapshot);

  aux_rel_ = table_open(aux_relid, lockmode);
  if (blockname) {
    ScanKeyData scankey[1];

    ScanKeyInit(&scankey[0], ANUM_PG_PAX_BLOCK_TABLES_PTBLOCKNAME, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum(blockname));
    scan_ = systable_beginscan(aux_rel_, aux_index_relid, true, snapshot, 1, scankey);
  } else {
    scan_ = systable_beginscan(aux_rel_, aux_index_relid, false, snapshot, 0, nullptr);
  }
}

HeapTuple ScanAuxContext::SearchMicroPartitionEntry() {
  Assert(aux_rel_ && scan_);
  return systable_getnext(scan_);
}

void ScanAuxContext::EndSearchMicroPartition(LOCKMODE lockmode) {
  Assert(aux_rel_ && scan_);

  systable_endscan(scan_);
  table_close(aux_rel_, lockmode);
  scan_ = nullptr;
  aux_rel_ = nullptr;
}

void PaxAuxRelationSetNewFilenode(Oid aux_relid) {
  Relation aux_rel;
  Oid toastrelid;
  ReindexParams reindex_params = {0};

  aux_rel = relation_open(aux_relid, AccessExclusiveLock);
  RelationSetNewRelfilenode(aux_rel, aux_rel->rd_rel->relpersistence);
  toastrelid = aux_rel->rd_rel->reltoastrelid;
  if (OidIsValid(toastrelid)) {
    Relation toast_rel;
    toast_rel = relation_open(toastrelid, AccessExclusiveLock);
    RelationSetNewRelfilenode(toast_rel, toast_rel->rd_rel->relpersistence);
    relation_close(toast_rel, NoLock);
  }
  if (aux_rel->rd_rel->relhasindex)
    reindex_relation(aux_relid, REINDEX_REL_PROCESS_TOAST, &reindex_params);
  pgstat_count_truncate(aux_rel);
  relation_close(aux_rel, NoLock);
}

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block, Snapshot snapshot) {
  struct ScanAuxContext context;
  HeapTuple tuple;
  Oid aux_relid;
  char block_name[NAMEDATALEN];
  bool ok;

  aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(pax_rel));
  snprintf(block_name, sizeof(block_name), "%u", block);

  context.BeginSearchMicroPartition(aux_relid, InvalidOid, snapshot, AccessShareLock, block_name);
  tuple = context.SearchMicroPartitionEntry();
  ok = HeapTupleIsValid(tuple);
  context.EndSearchMicroPartition(NoLock);

  return ok;
}

static void CPaxCopyPaxBlockEntry(Relation old_relation,
                                  Relation new_relation) {
  HeapTuple tuple;
  SysScanDesc pax_scan;
  Relation old_aux_rel, new_aux_rel;
  Oid old_aux_relid = 0, new_aux_relid = 0;

  old_aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(old_relation));
  new_aux_relid = ::paxc::GetPaxAuxRelid(RelationGetRelid(new_relation));
  old_aux_rel = table_open(old_aux_relid, RowExclusiveLock);
  new_aux_rel = table_open(new_aux_relid, RowExclusiveLock);

  pax_scan = systable_beginscan(old_aux_rel, InvalidOid, false, NULL, 0, NULL);
  while ((tuple = systable_getnext(pax_scan)) != NULL) {
    CatalogTupleInsert(new_aux_rel, tuple);
  }
  systable_endscan(pax_scan);
  table_close(old_aux_rel, RowExclusiveLock);
  table_close(new_aux_rel, RowExclusiveLock);
}

}  // namespace paxc

namespace cbdb {
Oid GetPaxAuxRelid(Oid relid) {
  CBDB_WRAP_START;
  { return ::paxc::GetPaxAuxRelid(relid); }
  CBDB_WRAP_END;
}

void DeleteMicroPartitionEntry(Oid pax_relid, Snapshot snapshot,
                                const std::string &blockname) {
  CBDB_WRAP_START;
  { paxc::DeleteMicroPartitionEntry(pax_relid, snapshot, blockname.c_str()); }
  CBDB_WRAP_END;
}

void InsertMicroPartitionPlaceHolder(Oid pax_relid, const std::string &blockname) {
  CBDB_WRAP_START;
  {
    Oid aux_relid;

    aux_relid = ::paxc::GetPaxAuxRelid(pax_relid);
    paxc::InsertMicroPartitionPlaceHolder(aux_relid, blockname.c_str());
  }
  CBDB_WRAP_END;
}
void InsertOrUpdateMicroPartitionEntry(const pax::WriteSummary &summary) {
  CBDB_WRAP_START;
  {
    Oid aux_relid;

    aux_relid = ::paxc::GetPaxAuxRelid(summary.rel_oid);
    paxc::InsertOrUpdateMicroPartitionPlaceHolder(aux_relid, summary.block_id.c_str(),
        summary.num_tuples, summary.file_size, summary.mp_stats);
  }
  CBDB_WRAP_END;
}

bool IsMicroPartitionVisible(Relation pax_rel, BlockNumber block, Snapshot snapshot) {
  CBDB_WRAP_START;
  { return paxc::IsMicroPartitionVisible(pax_rel, block, snapshot); }
  CBDB_WRAP_END;
}

static void PaxTransactionalTruncateTable(Oid aux_relid) {
  CBDB_WRAP_START;
  { paxc::CPaxTransactionalTruncateTable(aux_relid); }
  CBDB_WRAP_END;
}

static void PaxNontransactionalTruncateTable(Relation rel) {
  CBDB_WRAP_START;
  { paxc::CPaxNontransactionalTruncateTable(rel); }
  CBDB_WRAP_END;
}

static void PaxCreateMicroPartitionTable(const Relation rel) {
  CBDB_WRAP_START;
  { paxc::CPaxCreateMicroPartitionTable(rel); }
  CBDB_WRAP_END;
}

static void PaxCopyPaxBlockEntry(Relation old_relation, Relation new_relation) {
  CBDB_WRAP_START;
  { paxc::CPaxCopyPaxBlockEntry(old_relation, new_relation); }
  CBDB_WRAP_END;
}
}  // namespace cbdb

namespace pax {

void CCPaxAuxTable::PaxAuxRelationNontransactionalTruncate(Relation rel) {
  cbdb::PaxNontransactionalTruncateTable(rel);

  // Delete all micro partition file on non-transactional truncate  but reserve
  // top level PAX file directory.
  PaxAuxRelationFileUnlink(rel->rd_node, rel->rd_backend, false);
}

void CCPaxAuxTable::PaxAuxRelationCopyData(Relation rel,
                                           const RelFileNode *newrnode,
                                           bool createnewpath) {
  std::string src_path;
  std::string dst_path;
  std::vector<std::string> filelist;

  Assert(rel && newrnode);

  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();

  src_path = cbdb::BuildPaxDirectoryPath(rel->rd_node, rel->rd_backend);
  Assert(!src_path.empty());

  dst_path = cbdb::BuildPaxDirectoryPath(*newrnode, rel->rd_backend);
  Assert(!dst_path.empty());

  if (src_path.empty() || dst_path.empty())
    CBDB_RAISE(cbdb::CException::ExType::kExTypeFileOperationError);

  // createnewpath is used to indicate if creating destination micropartition
  // file directory and storage file for copying or not.
  // 1. For RelationCopyData case, createnewpath should be set as true to
  // explicitly create a new destination directory under
  //    new tablespace path pg_tblspc/.
  // 2. For RelationCopyDataForCluster case, createnewpath should be set as
  // false cause the destination directory was already
  //    created with a new temp table by previously calling
  //    PaxAuxRelationSetNewFilenode.
  if (createnewpath) {
    // create pg_pax_table relfilenode file and dbid directory.
    cbdb::RelationCreateStorageDirectory(*newrnode, rel->rd_rel->relpersistence,
                                         SMGR_MD, rel);
    // create micropartition file destination folder for copying.
    CBDB_CHECK((fs->CreateDirectory(dst_path) == 0),
               cbdb::CException::ExType::kExTypeIOError);
  }

  // Get micropatition file source folder filename list for copying, if file
  // list is empty then skip copying file directly.
  filelist = fs->ListDirectory(src_path);
  if (filelist.empty()) return;

  for (auto &iter : filelist) {
    Assert(!iter.empty());
    std::string src_file = src_path;
    std::string dst_file = dst_path;
    src_file.append("/");
    src_file.append(iter);
    dst_file.append("/");
    dst_file.append(iter);
    fs->CopyFile(src_file, dst_file);
  }

  // TODO(Tony) : here need to implement pending delete srcPath after set new
  // tablespace.
}

void CCPaxAuxTable::PaxAuxRelationCopyDataForCluster(Relation old_rel,
                                                     Relation new_rel) {
  PaxAuxRelationCopyData(old_rel, &new_rel->rd_node, false);
  cbdb::PaxCopyPaxBlockEntry(old_rel, new_rel);
  // TODO(Tony) : here need to implement PAX re-organize semantics logic.
}

void CCPaxAuxTable::PaxAuxRelationFileUnlink(RelFileNode node,
                                             BackendId backend,
                                             bool delete_topleveldir) {
  std::string relpath;
  FileSystem *fs = pax::Singleton<LocalFileSystem>::GetInstance();
  // Delete all micro partition file directory.
  relpath = cbdb::BuildPaxDirectoryPath(node, backend);
  fs->DeleteDirectory(relpath, delete_topleveldir);
}

}  // namespace pax
