#include "access/pax_inserter.h"

#include <string>
#include <utility>

#include "access/pax_dml_state.h"
#include "access/pax_partition.h"
#include "access/paxc_rel_options.h"
#include "catalog/pax_aux_table.h"
#include "comm/cbdb_wrappers.h"
#include "storage/micro_partition_stats.h"
#include "storage/strategy.h"

namespace pax {

CPaxInserter::CPaxInserter(Relation rel)
    : rel_(rel), insert_count_(0), part_obj_(nullptr), writer_(nullptr) {
  part_obj_ = new PartitionObject();
  auto ok = part_obj_->Initialize(rel_);
  if (ok) {
    writer_ = new TableParitionWriter(rel, part_obj_);
  } else {
    // fallback to TableWriter
    writer_ = new TableWriter(rel);
    part_obj_->Release();
    delete part_obj_;
    part_obj_ = nullptr;
  }

  writer_->SetWriteSummaryCallback(&cbdb::InsertOrUpdateMicroPartitionEntry)
      ->SetFileSplitStrategy(new PaxDefaultSplitStrategy())
      ->SetStatsCollector(new MicroPartitionStats())
      ->Open();
}

void CPaxInserter::InsertTuple(Relation relation, TupleTableSlot *slot,
                               CommandId /*cid*/, int /*options*/,
                               BulkInsertState /*bistate*/) {
  Assert(relation == rel_);
  slot->tts_tableOid = cbdb::RelationGetRelationId(relation);

  if (!TTS_IS_VIRTUAL(slot)) {
    slot_getallattrs(slot);
  }

  CTupleSlot cslot(slot);
  writer_->WriteTuple(&cslot);
}

void CPaxInserter::MultiInsert(Relation relation, TupleTableSlot **slots,
                               int ntuples, CommandId cid, int options,
                               BulkInsertState bistate) {
  CPaxInserter *inserter =
      pax::CPaxDmlStateLocal::Instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  for (int i = 0; i < ntuples; i++) {
    inserter->InsertTuple(relation, slots[i], cid, options, bistate);
  }
}

void CPaxInserter::FinishBulkInsert(Relation relation, int /*options*/) {
  pax::CPaxDmlStateLocal::Instance()->FinishDmlState(relation, CMD_INSERT);
}

void CPaxInserter::FinishInsert() {
  writer_->Close();
  delete writer_;
  writer_ = nullptr;

  if (part_obj_) {
    part_obj_->Release();
    delete part_obj_;
    part_obj_ = nullptr;
  }
}

void CPaxInserter::TupleInsert(Relation relation, TupleTableSlot *slot,
                               CommandId cid, int options,
                               BulkInsertState bistate) {
  CPaxInserter *inserter = CPaxDmlStateLocal::Instance()->GetInserter(relation);
  Assert(inserter != nullptr);

  inserter->InsertTuple(relation, slot, cid, options, bistate);
}

}  // namespace pax