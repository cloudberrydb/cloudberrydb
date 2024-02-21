#include "comm/cbdb_wrappers.h"

#include "comm/paxc_wrappers.h"
#include "storage/paxc_block_map_manager.h"
extern "C" {
const char *progname;
}

namespace cbdb {

MemoryContext pax_memory_context = nullptr;

CAutoExceptionStack::CAutoExceptionStack(void **global_exception_stack,
                                         void **global_error_context_stack)
    : m_global_exception_stack_(global_exception_stack),
      m_global_error_context_stack_(global_error_context_stack),
      m_exception_stack_(*global_exception_stack),
      m_error_context_stack_(*global_error_context_stack) {}

CAutoExceptionStack::~CAutoExceptionStack() {
  *m_global_exception_stack_ = m_exception_stack_;
  *m_global_error_context_stack_ = m_error_context_stack_;
}

// set the exception stack to the given address
void CAutoExceptionStack::SetLocalJmp(void *local_jump) {
  *m_global_exception_stack_ = local_jump;
}

void *MemCtxAlloc(MemoryContext ctx, size_t size) {
  CBDB_WRAP_START;
  {
    { return MemoryContextAlloc(ctx, (Size)size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *Palloc(size_t size) {
  CBDB_WRAP_START;
  {
#ifdef RUN_GTEST
    if (TopMemoryContext == nullptr) {
      MemoryContextInit();
    }
#endif
    { return palloc(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *Palloc0(size_t size) {
  CBDB_WRAP_START;
  {
#ifdef RUN_GTEST
    if (TopMemoryContext == nullptr) {
      MemoryContextInit();
    }
#endif
    { return palloc0(size); }
  }
  CBDB_WRAP_END;
  return nullptr;
}

void *RePalloc(void *ptr, size_t size) {
  CBDB_WRAP_START;
  { return repalloc(ptr, size); }
  CBDB_WRAP_END;
  return nullptr;
}

void Pfree(void *ptr) {
#ifdef RUN_GTEST
  if (ptr == nullptr) {
    return;
  }
#endif
  CBDB_WRAP_START;
  { pfree(ptr); }
  CBDB_WRAP_END;
}

}  // namespace cbdb

HTAB *cbdb::HashCreate(const char *tabname, int64 nelem, const HASHCTL *info,
                       int flags) {
  CBDB_WRAP_START;
  { return hash_create(tabname, nelem, info, flags); }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::HashSearch(HTAB *hashp, const void *key_ptr, HASHACTION action,
                       bool *found_ptr) {
  CBDB_WRAP_START;
  { return hash_search(hashp, key_ptr, action, found_ptr); }
  CBDB_WRAP_END;
  return nullptr;
}

MemoryContext cbdb::AllocSetCtxCreate(MemoryContext parent, const char *name,
                                      Size min_context_size,
                                      Size init_block_size,
                                      Size max_block_size) {
  CBDB_WRAP_START;
  {
    return AllocSetContextCreateInternal(parent, name, min_context_size,
                                         init_block_size, max_block_size);
  }
  CBDB_WRAP_END;
  return nullptr;
}

void cbdb::MemoryCtxDelete(MemoryContext memory_context) {
  CBDB_WRAP_START;
  { MemoryContextDelete(memory_context); }
  CBDB_WRAP_END;
}

void cbdb::MemoryCtxRegisterResetCallback(MemoryContext context,
                                          MemoryContextCallback *cb) {
  CBDB_WRAP_START;
  { MemoryContextRegisterResetCallback(context, cb); }
  CBDB_WRAP_END;
}

Oid cbdb::RelationGetRelationId(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetRelid(rel); }
  CBDB_WRAP_END;
}

#ifdef RUN_GTEST
Datum cbdb::DatumFromCString(const char *src, size_t length) {
  CBDB_WRAP_START;
  {
    text *result = reinterpret_cast<text *>(palloc(length + VARHDRSZ));
    SET_VARSIZE(result, length + VARHDRSZ);
    memcpy(VARDATA(result), src, length);
    return PointerGetDatum(result);
  }
  CBDB_WRAP_END;
  return 0;
}

Datum cbdb::DatumFromPointer(const void *p, int16 typlen) {
  CBDB_WRAP_START;
  {
    char *resultptr;
    resultptr = reinterpret_cast<char *>(palloc(typlen));
    memcpy(resultptr, p, typlen);
    return PointerGetDatum(resultptr);
  }
  CBDB_WRAP_END;
  return 0;
}
#endif

struct varlena *cbdb::PgDeToastDatumPacked(struct varlena *datum) {
  CBDB_WRAP_START;
  { return pg_detoast_datum_packed(datum); }
  CBDB_WRAP_END;
  return nullptr;
}

void *cbdb::PointerAndLenFromDatum(Datum d, int *len) {
  struct varlena *vl = nullptr;
  CBDB_WRAP_START;
  {
    vl = (struct varlena *)DatumGetPointer(d);
    *len = VARSIZE_ANY(vl);
    return static_cast<void *>(vl);
  }
  CBDB_WRAP_END;
}

void cbdb::SlotGetMissingAttrs(TupleTableSlot *slot, int start_attno,
                               int last_attno) {
  CBDB_WRAP_START;
  { slot_getmissingattrs(slot, start_attno, last_attno); }
  CBDB_WRAP_END;
}

void cbdb::RelationCreateStorageDirectory(RelFileNode rnode,
                                          char relpersistence,
                                          SMgrImpl smgr_which, Relation rel) {
  CBDB_WRAP_START;
  {
    SMgrRelation srel =
        RelationCreateStorage(rnode, relpersistence, smgr_which, rel);
    smgrclose(srel);
  }
  CBDB_WRAP_END;
}

void cbdb::RelationDropStorageDirectory(Relation rel) {
  CBDB_WRAP_START;
  { RelationDropStorage(rel); }
  CBDB_WRAP_END;
}

int cbdb::PathNameCreateDir(const char *path) {
  CBDB_WRAP_START;
  { return MakePGDirectory(path); }
  CBDB_WRAP_END;
}

HeapTuple cbdb::SearchSysCache(Relation rel, SysCacheIdentifier id) {
  CBDB_WRAP_START;
  { return SearchSysCache1(id, RelationGetRelid(rel)); }
  CBDB_WRAP_END;
}

bool cbdb::TupleIsValid(HeapTuple tupcache) {
  CBDB_WRAP_START;
  { return HeapTupleIsValid(tupcache); }
  CBDB_WRAP_END;
}

void cbdb::ReleaseTupleCache(HeapTuple tupcache) {
  CBDB_WRAP_START;
  { ReleaseSysCache(tupcache); }
  CBDB_WRAP_END;
}

void cbdb::PathNameDeleteDir(const char *path, bool delete_topleveldir) {
  CBDB_WRAP_START;
  { paxc::DeletePaxDirectoryPath(path, delete_topleveldir); }
  CBDB_WRAP_END;
}

void cbdb::CopyFile(const char *srcsegpath, const char *dstsegpath) {
  CBDB_WRAP_START;
  { paxc::CopyFile(srcsegpath, dstsegpath); }
  CBDB_WRAP_END;
}

void cbdb::MakedirRecursive(const char *path) {
  CBDB_WRAP_START;
  { paxc::MakedirRecursive(path); }
  CBDB_WRAP_END;
}

std::string cbdb::BuildPaxDirectoryPath(RelFileNode rd_node,
                                        BackendId rd_backend) {
  CBDB_WRAP_START;
  {
    char *tmp_str = paxc::BuildPaxDirectoryPath(rd_node, rd_backend);
    std::string ret_str(tmp_str);
    pfree(tmp_str);
    return ret_str;
  }
  CBDB_WRAP_END;
}

std::string cbdb::BuildPaxFilePath(const std::string &rel_path,
                                   const std::string &block_id) {
  Assert(!rel_path.empty());
  return rel_path + "/" + block_id;
}

int cbdb::RelationGetAttributesNumber(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetNumberOfAttributes(rel); }
  CBDB_WRAP_END;
}

StdRdOptions **cbdb::RelGetAttributeOptions(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetAttributeOptions(rel); }
  CBDB_WRAP_END;
}

TupleDesc cbdb::RelationGetTupleDesc(Relation rel) {
  CBDB_WRAP_START;
  { return RelationGetDescr(rel); }
  CBDB_WRAP_END;
}

bool cbdb::IsSystemAttrNumExist(struct PaxcExtractcolumnContext *context,
                                AttrNumber number) {
  Assert(number < 0 && number > FirstLowInvalidHeapAttributeNumber && context);
  return context->system_attr_number_mask[~number];
}

extern "C" {

static bool paxc_extractcolumns_walker(  // NOLINT
    Node *node, struct PaxcExtractcolumnContext *ec_ctx) {
  if (node == NULL) {
    return false;
  }

  if (IsA(node, Var)) {
    Var *var = (Var *)node;

    if (IS_SPECIAL_VARNO(var->varno)) return false;

    if (var->varattno < 0) {
      Assert(var->varattno > FirstLowInvalidHeapAttributeNumber);
      ec_ctx->system_attr_number_mask[~var->varattno] = true;
    } else if (ec_ctx->cols) {
      if (var->varattno == 0) {
        // If all attributes are included,
        // set all entries in mask to true.
        for (int attno = 0; attno < ec_ctx->natts; attno++)
          ec_ctx->cols[attno] = true;
        ec_ctx->found = true;
      } else if (var->varattno <= ec_ctx->natts) {
        ec_ctx->cols[var->varattno - 1] = true;
        ec_ctx->found = true;
      }
      // Still need fill `system_attr_number_mask`
      // Let this case return false
    }

    return false;
  }

  return expression_tree_walker(node, (bool (*)())paxc_extractcolumns_walker,
                                (void *)ec_ctx);
}

};  // extern "C"

bool cbdb::ExtractcolumnsFromNode(Node *expr,
                                  struct PaxcExtractcolumnContext *ec_ctx) {
  CBDB_WRAP_START;
  {
    paxc_extractcolumns_walker(expr, ec_ctx);
    return ec_ctx->found;
  }
  CBDB_WRAP_END;
}

bool cbdb::ExtractcolumnsFromNode(Node *expr, bool *cols, int natts) {
  CBDB_WRAP_START;
  { return extractcolumns_from_node(expr, cols, natts); }
  CBDB_WRAP_END;
}

bool cbdb::MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid *opfamily, FmgrInfo *finfo,
                                     StrategyNumber strategynum) {
  CBDB_WRAP_START;
  {
    return paxc::MinMaxGetStrategyProcinfo(atttypid, subtype, opfamily, finfo,
                                           strategynum);
  }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall1Coll(FmgrInfo *flinfo, Oid collation, Datum arg1) {
  CBDB_WRAP_START;
  { return ::FunctionCall1Coll(flinfo, collation, arg1); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                              Datum arg2) {
  CBDB_WRAP_START;
  { return ::FunctionCall2Coll(flinfo, collation, arg1, arg2); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall3Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                              Datum arg2, Datum arg3) {
  CBDB_WRAP_START;
  { return ::FunctionCall3Coll(flinfo, collation, arg1, arg2, arg3); }
  CBDB_WRAP_END;
}

Datum cbdb::FunctionCall4Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                              Datum arg2, Datum arg3, Datum arg4) {
  CBDB_WRAP_START;
  { return ::FunctionCall4Coll(flinfo, collation, arg1, arg2, arg3, arg4); }
  CBDB_WRAP_END;
}

SysScanDesc cbdb::SystableBeginScan(Relation rel, Oid index_id, bool index_ok,
                                    Snapshot snapshot, int n_keys,
                                    ScanKey keys) {
  CBDB_WRAP_START;
  {
    return systable_beginscan(rel, index_id, index_ok, snapshot, n_keys, keys);
  }
  CBDB_WRAP_END;
}

HeapTuple cbdb::SystableGetNext(SysScanDesc desc) {
  CBDB_WRAP_START;
  { return systable_getnext(desc); }
  CBDB_WRAP_END;
}

void cbdb::SystableEndScan(SysScanDesc desc) {
  CBDB_WRAP_START;
  { return systable_endscan(desc); }
  CBDB_WRAP_END;
}

Datum cbdb::HeapGetAttr(HeapTuple tup, int attnum, TupleDesc tuple_desc,
                        bool *isnull) {
  CBDB_WRAP_START;
  { return heap_getattr(tup, attnum, tuple_desc, isnull); }
  CBDB_WRAP_END;
}

Relation cbdb::TableOpen(Oid relid, LOCKMODE lockmode) {
  CBDB_WRAP_START;
  { return table_open(relid, lockmode); }
  CBDB_WRAP_END;
}

void cbdb::TableClose(Relation rel, LOCKMODE lockmode) {
  CBDB_WRAP_START;
  { return table_close(rel, lockmode); }
  CBDB_WRAP_END;
}
