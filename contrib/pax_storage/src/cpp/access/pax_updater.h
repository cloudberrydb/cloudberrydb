#pragma once

#include "comm/cbdb_api.h"

namespace pax {
class CPaxUpdater final {
 public:
  static TM_Result UpdateTuple(const Relation relation, const ItemPointer otid,
                               TupleTableSlot *slot, const CommandId cid,
                               const Snapshot snapshot,
                               const Snapshot crosscheck, const bool wait,
                               TM_FailureData *tmfd, LockTupleMode *lockmode,
                               bool *update_indexes);
};
}  // namespace pax
