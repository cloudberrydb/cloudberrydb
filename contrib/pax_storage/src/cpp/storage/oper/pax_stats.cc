#include "storage/oper/pax_stats.h"

#include "comm/cbdb_wrappers.h"

namespace pax {

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid collid,
                               OperMinMaxFunc &func,
                               StrategyNumber strategynum) {
  OperMinMaxKey key;

  if (!CollateIsSupport(collid)) {
    return false;
  }

  key = {atttypid, subtype, strategynum};
  auto it = min_max_opers.find(key);
  if (it != min_max_opers.end()) {
    func = it->second;
    return true;
  }
  return false;
}

bool GetStrategyProcinfo(Oid typid, Oid subtype, Oid *opfamily,
                         std::pair<FmgrInfo, FmgrInfo> &finfos) {
  Oid opfamily1;
  Oid opfamily2;

  auto ok =
      cbdb::MinMaxGetStrategyProcinfo(typid, subtype, &opfamily1, &finfos.first,
                                      BTLessStrategyNumber) &&
      cbdb::MinMaxGetStrategyProcinfo(typid, subtype, &opfamily2,
                                      &finfos.second, BTGreaterStrategyNumber);
  if (ok) {
    Assert(opfamily1 == opfamily2);
    Assert(OidIsValid(opfamily1));
    *opfamily = opfamily1;
  }
  return ok;
}

bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs) {
  return MinMaxGetStrategyProcinfo(typid, subtype, InvalidOid, funcs.first,
                                   BTLessStrategyNumber) &&
         MinMaxGetStrategyProcinfo(typid, subtype, InvalidOid, funcs.second,
                                   BTGreaterStrategyNumber);
}

bool CollateIsSupport(Oid collid) {
  return collid == InvalidOid ||  // Allow no setting collate
         collid == DEFAULT_COLLATION_OID || collid == C_COLLATION_OID ||
         collid == POSIX_COLLATION_OID;
}

}  // namespace pax
