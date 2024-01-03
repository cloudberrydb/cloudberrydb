#include "access/pax_updater.h"

#include "access/pax_deleter.h"
#include "access/pax_dml_state.h"
#include "access/pax_inserter.h"

namespace pax {
TM_Result CPaxUpdater::UpdateTuple(
    const Relation relation, const ItemPointer otid, TupleTableSlot *slot,
    const CommandId cid, const Snapshot snapshot, const Snapshot /*crosscheck*/,
    const bool /*wait*/, TM_FailureData * tmfd,
    LockTupleMode * lockmode, bool *update_indexes) {
  TM_Result result;

  auto dml_state = CPaxDmlStateLocal::Instance();
  auto deleter = dml_state->GetDeleter(relation, snapshot);
  auto inserter = dml_state->GetInserter(relation);

  Assert(deleter != nullptr);
  Assert(inserter != nullptr);

  *lockmode = LockTupleExclusive;
  result = deleter->MarkDelete(otid);

  if (result == TM_Ok) {
    inserter->InsertTuple(relation, slot, cid, 0, nullptr);
    *update_indexes = true;
  } else {
    // FIXME: set tmfd correctly.
    // FYI, ao ignores both tmfd and lockmode
    tmfd->ctid = *otid;
    *update_indexes = false;
  }
  // TODO(gongxun): update pgstat info
  return result;
}
}  // namespace pax
