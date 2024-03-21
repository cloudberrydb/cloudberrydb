#include "access/pax_dml_state.h"

#define PAX_DEFAULT_GP_INTERCONNECT_QUEUE_DEPTH 64
#define PAX_DEFAULT_COLUMN_NUM_OF_WIDE_TABLE 64

namespace pax {
// class CPaxDmlStateLocal

void CPaxDmlStateLocal::DmlStateResetCallback(void * /*arg*/) {
  pax::CPaxDmlStateLocal::Instance()->Reset();
}

void CPaxDmlStateLocal::InitDmlState(Relation rel, CmdType operation) {
  if (Gp_role == GP_ROLE_DISPATCH &&
      Gp_interconnect_queue_depth < PAX_DEFAULT_GP_INTERCONNECT_QUEUE_DEPTH &&
      rel->rd_att->natts > PAX_DEFAULT_COLUMN_NUM_OF_WIDE_TABLE) {
    elog(WARNING,
         "in pax table am, we recommend that gp_interconnect_queue_depth is a "
         "value greater than or equal to 64, but current value is %d",
         Gp_interconnect_queue_depth);
  }

  if (!dml_descriptor_tab_) {
    HASHCTL hash_ctl;
    Assert(!cbdb::pax_memory_context);

    cbdb::pax_memory_context = AllocSetContextCreate(
        CurrentMemoryContext, "Pax Storage", PAX_ALLOCSET_DEFAULT_SIZES);

    cbdb::MemoryCtxRegisterResetCallback(cbdb::pax_memory_context, &cb_);

    memset(&hash_ctl, 0, sizeof(hash_ctl));
    hash_ctl.keysize = sizeof(Oid);
    hash_ctl.entrysize = sizeof(PaxDmlState);
    hash_ctl.hcxt = cbdb::pax_memory_context;
    dml_descriptor_tab_ = cbdb::HashCreate(
        "Pax DML state", 128, &hash_ctl, HASH_CONTEXT | HASH_ELEM | HASH_BLOBS);
  }

  EntryDmlState(cbdb::RelationGetRelationId(rel));
}

void CPaxDmlStateLocal::FinishDmlState(Relation rel, CmdType /*operation*/) {
  PaxDmlState *state;
  state = RemoveDmlState(cbdb::RelationGetRelationId(rel));

  if (state == nullptr) {
    return;
  }

  if (state->deleter) {
    // TODO(gongxun): deleter finish
    state->deleter->ExecDelete();

    PAX_DELETE(state->deleter);
    state->deleter = nullptr;
    // FIXME: it's update operation, maybe we should do something here
  }

  if (state->inserter) {
    MemoryContext old_ctx;
    Assert(cbdb::pax_memory_context);

    old_ctx = MemoryContextSwitchTo(cbdb::pax_memory_context);
    state->inserter->FinishInsert();
    PAX_DELETE(state->inserter);
    state->inserter = nullptr;
    MemoryContextSwitchTo(old_ctx);
  }
}

CPaxInserter *CPaxDmlStateLocal::GetInserter(Relation rel) {
  PaxDmlState *state;
  state = FindDmlState(cbdb::RelationGetRelationId(rel));
  // TODO(gongxun): switch memory context??
  if (state->inserter == nullptr) {
    state->inserter = PAX_NEW<CPaxInserter>(rel);
  }
  return state->inserter;
}

CPaxDeleter *CPaxDmlStateLocal::GetDeleter(Relation rel, Snapshot snapshot) {
  PaxDmlState *state;
  state = FindDmlState(cbdb::RelationGetRelationId(rel));
  // TODO(gongxun): switch memory context??
  if (state->deleter == nullptr) {
    state->deleter = PAX_NEW<CPaxDeleter>(rel, snapshot);
  }
  return state->deleter;
}

void CPaxDmlStateLocal::Reset() {
  last_used_state_ = nullptr;
  dml_descriptor_tab_ = nullptr;
  cbdb::pax_memory_context = nullptr;
}

CPaxDmlStateLocal::CPaxDmlStateLocal()
    : last_used_state_(nullptr),
      dml_descriptor_tab_(nullptr),
      cb_{.func = DmlStateResetCallback, .arg = NULL} {}

PaxDmlState *CPaxDmlStateLocal::EntryDmlState(const Oid &oid) {
  PaxDmlState *state;
  bool found;
  Assert(this->dml_descriptor_tab_);

  state = reinterpret_cast<PaxDmlState *>(
      cbdb::HashSearch(this->dml_descriptor_tab_, &oid, HASH_ENTER, &found));
  state->inserter = nullptr;
  state->deleter = nullptr;
  Assert(!found);

  this->last_used_state_ = state;
  return state;
}

PaxDmlState *CPaxDmlStateLocal::RemoveDmlState(const Oid &oid) {
  Assert(this->dml_descriptor_tab_);

  PaxDmlState *state;
  state = reinterpret_cast<PaxDmlState *>(
      cbdb::HashSearch(this->dml_descriptor_tab_, &oid, HASH_REMOVE, NULL));

  if (!state) return NULL;

  if (this->last_used_state_ && this->last_used_state_->oid == oid)
    this->last_used_state_ = NULL;

  return state;
}

PaxDmlState *CPaxDmlStateLocal::FindDmlState(const Oid &oid) {
  Assert(this->dml_descriptor_tab_);

  if (this->last_used_state_ && this->last_used_state_->oid == oid)
    return last_used_state_;

  PaxDmlState *state;
  state = reinterpret_cast<PaxDmlState *>(
      cbdb::HashSearch(this->dml_descriptor_tab_, &oid, HASH_FIND, NULL));
  Assert(state);

  this->last_used_state_ = state;
  return state;
}

}  // namespace pax
