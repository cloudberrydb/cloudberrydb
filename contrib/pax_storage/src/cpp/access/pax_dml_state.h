#pragma once

#include "comm/cbdb_api.h"

#include <memory>

#include "access/pax_deleter.h"
#include "access/pax_inserter.h"
#include "comm/cbdb_wrappers.h"
#include "comm/singleton.h"

namespace pax {
struct PaxDmlState {
  Oid oid;
  CPaxInserter *inserter;
  CPaxDeleter *deleter;
};

class CPaxDmlStateLocal final {
  friend class Singleton<CPaxDmlStateLocal>;

 public:
  static CPaxDmlStateLocal *Instance() {
    return Singleton<CPaxDmlStateLocal>::GetInstance();
  }

  ~CPaxDmlStateLocal() = default;

  void InitDmlState(Relation rel, CmdType operation);
  void FinishDmlState(Relation rel, CmdType operation);

  CPaxInserter *GetInserter(Relation rel);
  CPaxDeleter *GetDeleter(Relation rel, Snapshot snapshot);

  void Reset();

  CPaxDmlStateLocal(const CPaxDmlStateLocal &) = delete;
  CPaxDmlStateLocal &operator=(const CPaxDmlStateLocal &) = delete;

 private:
  CPaxDmlStateLocal();
  static void DmlStateResetCallback(void * /*arg*/);

  PaxDmlState *EntryDmlState(const Oid &oid);
  PaxDmlState *FindDmlState(const Oid &oid);
  PaxDmlState *RemoveDmlState(const Oid &oid);

 private:
  PaxDmlState *last_used_state_;
  HTAB *dml_descriptor_tab_;
  MemoryContextCallback cb_;
};

}  // namespace pax
