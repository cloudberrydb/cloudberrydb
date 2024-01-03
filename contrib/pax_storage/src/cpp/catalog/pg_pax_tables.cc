#include "catalog/pg_pax_tables.h"

#include "comm/cbdb_api.h"

namespace paxc {

void InsertPaxTablesEntry(Oid relid, Oid blocksrelid, Node *partitionspec) {
  Relation rel;
  TupleDesc desc;
  HeapTuple tuple;
  bool nulls[NATTS_PG_PAX_TABLES];
  Datum values[NATTS_PG_PAX_TABLES];

  rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  desc = RelationGetDescr(rel);
  Assert(desc->natts == NATTS_PG_PAX_TABLES);

  values[ANUM_PG_PAX_TABLES_RELID - 1] = ObjectIdGetDatum(relid);
  values[ANUM_PG_PAX_TABLES_AUXRELID - 1] = ObjectIdGetDatum(blocksrelid);
  nulls[ANUM_PG_PAX_TABLES_RELID - 1] = false;
  nulls[ANUM_PG_PAX_TABLES_AUXRELID - 1] = false;

  if (partitionspec) {
    values[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] =
        CStringGetTextDatum(nodeToString(partitionspec));
    nulls[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = false;
  } else {
    values[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = 0;
    nulls[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = true;
  }
  tuple = heap_form_tuple(desc, values, nulls);

  /* insert a new tuple */
  CatalogTupleInsert(rel, tuple);

  table_close(rel, NoLock);
}

void GetPaxTablesEntryAttributes(Oid relid, Oid *blocksrelid,
                                 Node **partitionspec) {
  Relation rel;
  ScanKeyData key[1];
  SysScanDesc scan;
  HeapTuple tuple;
  bool isnull;

  rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);

  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(relid));

  scan = systable_beginscan(rel, PAX_TABLES_RELID_INDEX_ID, true, NULL, 1, key);
  tuple = systable_getnext(scan);
  if (!HeapTupleIsValid(tuple))
    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                    errmsg("pax table relid \"%d\" does not exist in "
                           "pg_pax_tables",
                           relid)));

  if (partitionspec) {
    Datum v;
    v = heap_getattr(tuple, ANUM_PG_PAX_TABLES_PARTITIONSPEC,
                     RelationGetDescr(rel), &isnull);
    *partitionspec = NULL;
    if (!isnull) {
      char *str = TextDatumGetCString(v);
      *partitionspec = (Node *)stringToNode(str);
      pfree(str);
    }
  }

  if (blocksrelid) {
    *blocksrelid = heap_getattr(tuple, ANUM_PG_PAX_TABLES_AUXRELID,
                                RelationGetDescr(rel), &isnull);
    if (isnull) ereport(ERROR, (errmsg("pg_pax_tables.auxrelid is null")));
  }

  /* Finish up scan and close pg_pax_tables catalog. */
  systable_endscan(scan);
  table_close(rel, NoLock);
}

void PaxInitializePartitionSpec(Relation paxrel, Node *part) {
  Relation rel;
  ScanKeyData key[1];
  SysScanDesc scan;
  HeapTuple oldtuple;
  TupleDesc desc;
  bool isnull;

  Assert(paxrel->rd_rel->relkind == RELKIND_RELATION ||
         paxrel->rd_rel->relkind == RELKIND_MATVIEW);
  Assert(paxrel->rd_options);

  rel = table_open(PAX_TABLES_RELATION_ID, RowExclusiveLock);
  desc = RelationGetDescr(rel);
  ScanKeyInit(&key[0], ANUM_PG_PAX_TABLES_RELID, BTEqualStrategyNumber, F_OIDEQ,
              ObjectIdGetDatum(RelationGetRelid(paxrel)));

  scan = systable_beginscan(rel, PAX_TABLES_RELID_INDEX_ID, true, NULL, 1, key);
  oldtuple = systable_getnext(scan);
  if (!HeapTupleIsValid(oldtuple)) elog(ERROR, "only support pax tables");

  (void)heap_getattr(oldtuple, ANUM_PG_PAX_TABLES_PARTITIONSPEC, desc, &isnull);
  if (isnull) {
    HeapTuple newtup;
    Datum values[NATTS_PG_PAX_TABLES];
    bool repl[NATTS_PG_PAX_TABLES];
    bool isnull[NATTS_PG_PAX_TABLES];

    memset(repl, false, sizeof(repl));
    values[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] =
        CStringGetTextDatum(nodeToString(part));
    repl[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = true;
    isnull[ANUM_PG_PAX_TABLES_PARTITIONSPEC - 1] = false;

    newtup = heap_modify_tuple(oldtuple, desc, values, isnull, repl);
    CatalogTupleUpdate(rel, &oldtuple->t_self, newtup);
    heap_freetuple(newtup);

    CommandCounterIncrement();
  } else {
    elog(ERROR, "existing pax table update partition spec?");
  }

  /* Finish up scan and close pg_pax_tables catalog. */
  systable_endscan(scan);
  table_close(rel, NoLock);
}

}  // namespace paxc
