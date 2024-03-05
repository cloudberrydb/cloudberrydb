#include <cstdio>

#include "stub.h"
#include "comm/gtest_wrappers.h"
#include "comm/cbdb_wrappers.h"
#include "catalog/pax_fastsequence.h"
#include "catalog/pax_aux_table.h"

bool MockMinMaxGetStrategyProcinfo(Oid, Oid, Oid *, FmgrInfo *,
                                   StrategyNumber) {
  return false;
}

int32 MockGetFastSequences(Oid) {
  static int32 mock_id = 0;
  return mock_id++;
}


void MockInsertMicroPartitionPlaceHolder(Oid, const std::string &) {
  // do nothing
}


void MockDeleteMicroPartitionEntry(Oid , Snapshot,
                               const char *) {
  // do nothing
}

void MockExecStoreVirtualTuple(TupleTableSlot *){
  // do nothing
}

std::string MockBuildPaxDirectoryPath(RelFileNode,
                                        BackendId) {
  return std::string(".");
}



// Mock global method which is not link from another libarays
void GlobalMock(Stub *stub) {
  stub->set(cbdb::MinMaxGetStrategyProcinfo, MockMinMaxGetStrategyProcinfo);
  stub->set(paxc::CPaxGetFastSequences, MockGetFastSequences);
  stub->set(cbdb::BuildPaxDirectoryPath, MockBuildPaxDirectoryPath);
  stub->set(cbdb::InsertMicroPartitionPlaceHolder, MockInsertMicroPartitionPlaceHolder);
  stub->set(cbdb::DeleteMicroPartitionEntry, MockDeleteMicroPartitionEntry);
  stub->set(ExecStoreVirtualTuple, MockExecStoreVirtualTuple);
}

int main(int argc, char **argv) {
  Stub *stub_global;
  stub_global = new Stub();
  testing::InitGoogleTest(&argc, argv);
  GlobalMock(stub_global);

  return RUN_ALL_TESTS();
}
