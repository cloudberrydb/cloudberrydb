#include "storage/pax_itemptr.h"

#include "comm/cbdb_wrappers.h"
#include "catalog/pax_fastsequence.h"

namespace pax {
std::string GenerateBlockID(Relation relation) {
  int32 seqno = -1;
  CBDB_WRAP_START;
  {
    seqno = paxc::CPaxGetFastSequences(RelationGetRelid(relation));
  }
  CBDB_WRAP_END;
  return std::to_string(seqno);
}
}  // namespace pax

