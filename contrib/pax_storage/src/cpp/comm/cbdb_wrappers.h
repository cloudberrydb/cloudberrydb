#pragma once

#include "comm/cbdb_api.h"

#include <cstddef>
#include <string>

#include "exceptions/CException.h"

struct PaxcExtractcolumnContext {
  // If cols set and call ExtractcolumnsFromNode with
  // `target list`. Then the cols will fill with projection mask.
  bool *cols = nullptr;
  int natts = 0;
  bool found = false;

  // This mask use to filter system attribute number.
  // (~AttrNumber) will be index, mapping the [0,
  // FirstLowInvalidHeapAttributeNumber) call `IsSystemAttrNumExist` to check
  // system-defined attributes set
  bool system_attr_number_mask[~FirstLowInvalidHeapAttributeNumber] = {
      0};  // NOLINT
};

namespace cbdb {

#define PAX_ALLOCSET_DEFAULT_MINSIZE ALLOCSET_DEFAULT_MINSIZE
#define PAX_ALLOCSET_DEFAULT_INITSIZE (8 * 1024)
#define PAX_ALLOCSET_DEFAULT_MAXSIZE (3 * 64 * 1024 * 1024)

#define PAX_ALLOCSET_DEFAULT_SIZES                             \
  PAX_ALLOCSET_DEFAULT_MINSIZE, PAX_ALLOCSET_DEFAULT_INITSIZE, \
      PAX_ALLOCSET_DEFAULT_MAXSIZE
extern MemoryContext pax_memory_context;

//---------------------------------------------------------------------------
//  @class:
//      CAutoExceptionStack
//
//  @doc:
//      Auto object for saving and restoring exception stack
//
//---------------------------------------------------------------------------
class CAutoExceptionStack final {
 public:
  CAutoExceptionStack(const CAutoExceptionStack &) = delete;
  CAutoExceptionStack(CAutoExceptionStack &&) = delete;
  void *operator new(std::size_t count, ...) = delete;
  void *operator new[](std::size_t count, ...) = delete;

  // ctor
  CAutoExceptionStack(void **global_exception_stack,
                      void **global_error_context_stack);

  // dtor
  ~CAutoExceptionStack();

  // set the exception stack to the given address
  void SetLocalJmp(void *local_jump);

 private:
  // address of the global exception stack value
  void **m_global_exception_stack_;

  // address of the global error context stack value
  void **m_global_error_context_stack_;

  // value of exception stack when object is created
  void *m_exception_stack_;

  // value of error context stack when object is created
  void *m_error_context_stack_;
};  // class CAutoExceptionStack

void *MemCtxAlloc(MemoryContext ctx, size_t size);
void *Palloc(size_t size);
void *Palloc0(size_t size);
void *RePalloc(void *ptr, size_t size);
void Pfree(void *ptr);

HTAB *HashCreate(const char *tabname, int64 nelem, const HASHCTL *info,
                 int flags);
void *HashSearch(HTAB *hashp, const void *key_ptr, HASHACTION action,
                 bool *found_ptr);
MemoryContext AllocSetCtxCreate(MemoryContext parent, const char *name,
                                Size min_context_size, Size init_block_size,
                                Size max_block_size);
void MemoryCtxDelete(MemoryContext memory_context);
void MemoryCtxRegisterResetCallback(MemoryContext context,
                                    MemoryContextCallback *cb);

Oid RelationGetRelationId(Relation rel);

static inline void *DatumToPointer(Datum d) noexcept {
  return DatumGetPointer(d);
}

static inline int8 DatumToInt8(Datum d) noexcept { return DatumGetInt8(d); }

static inline int16 DatumToInt16(Datum d) noexcept { return DatumGetInt16(d); }

static inline int32 DatumToInt32(Datum d) noexcept { return DatumGetInt32(d); }

static inline int64 DatumToInt64(Datum d) noexcept { return DatumGetInt64(d); }

static inline Datum Int8ToDatum(int8 d) noexcept { return Int8GetDatum(d); }

static inline Datum Int16ToDatum(int16 d) noexcept { return Int16GetDatum(d); }

static inline Datum Int32ToDatum(int32 d) noexcept { return Int32GetDatum(d); }

static inline Datum Int64ToDatum(int64 d) noexcept { return Int64GetDatum(d); }

Numeric DatumToNumeric(Datum d);

void *PointerAndLenFromDatum(Datum d, int *len);

void SlotGetMissingAttrs(TupleTableSlot *slot, int start_attno, int last_attno);

#ifdef RUN_GTEST
Datum DatumFromCString(const char *src, size_t length);

Datum DatumFromPointer(const void *p, int16 typlen);
#endif

struct varlena *PgDeToastDatum(struct varlena *datum);

struct varlena *PgDeToastDatumPacked(struct varlena *datum);

void RelationCreateStorageDirectory(RelFileNode rnode, char relpersistence,
                                    SMgrImpl smgr_which, Relation rel);

bool TupleIsValid(HeapTuple tupcache);

void ReleaseTupleCache(HeapTuple tupcache);

void RelationDropStorageDirectory(Relation rel);

int PathNameCreateDir(const char *path);

HeapTuple SearchSysCache(Relation rel, SysCacheIdentifier id);

void PathNameDeleteDir(const char *path, bool delete_topleveldir);

void CopyFile(const char *srcsegpath, const char *dstsegpath);

void MakedirRecursive(const char *path);

std::string BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend);

std::string BuildPaxFilePath(const std::string &rel_path,
                             const std::string &block_id);

int RelationGetAttributesNumber(Relation rel);

StdRdOptions **RelGetAttributeOptions(Relation rel);
TupleDesc RelationGetTupleDesc(Relation rel);

bool ExtractcolumnsFromNode(Node *expr,
                            struct PaxcExtractcolumnContext *ec_ctx);

bool IsSystemAttrNumExist(struct PaxcExtractcolumnContext *context,
                          AttrNumber number);

bool ExtractcolumnsFromNode(Node *expr, bool *cols, int natts);

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid *opfamily,
                               FmgrInfo *finfo, StrategyNumber strategynum);

Datum FunctionCall1Coll(FmgrInfo *flinfo, Oid collation, Datum arg1);

Datum FunctionCall2Coll(FmgrInfo *flinfo, Oid collation, Datum arg1,
                        Datum arg2);

Datum FunctionCall3Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2,
                        Datum arg3);

Datum FunctionCall4Coll(FmgrInfo *flinfo, Oid collation, Datum arg1, Datum arg2,
                        Datum arg3, Datum arg4);

SysScanDesc SystableBeginScan(Relation rel, Oid index_id, bool index_ok,
                              Snapshot snapshot, int n_keys, ScanKey keys);

HeapTuple SystableGetNext(SysScanDesc desc);

void SystableEndScan(SysScanDesc desc);

Datum HeapGetAttr(HeapTuple tup, int attnum, TupleDesc tuple_desc,
                  bool *isnull);

Relation TableOpen(Oid relid, LOCKMODE lockmode);

void TableClose(Relation rel, LOCKMODE lockmode);

void RelOpenSmgr(Relation rel);

void RelCloseSmgr(Relation rel);

void RelDropStorage(Relation rel);

}  // namespace cbdb

// clang-format off
#define CBDB_WRAP_START                                           \
  sigjmp_buf local_sigjmp_buf;                                    \
  {                                                               \
      cbdb::CAutoExceptionStack aes(                              \
          reinterpret_cast<void **>(&PG_exception_stack),         \
          reinterpret_cast<void **>(&error_context_stack));       \
      if (0 == sigsetjmp(local_sigjmp_buf, 0))                    \
      {                                                           \
          aes.SetLocalJmp(&local_sigjmp_buf)

#define CBDB_WRAP_END                                             \
  }                                                               \
  else                                                            \
  {                                                               \
      CBDB_RAISE(cbdb::CException::ExType::kExTypeCError);         \
  }                                                               \
  }
// clang-format on
