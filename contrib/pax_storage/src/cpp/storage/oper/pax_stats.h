#pragma once
#include "comm/cbdb_api.h"

#include "storage/oper/pax_oper.h"

namespace pax {

bool MinMaxGetStrategyProcinfo(Oid atttypid, Oid subtype, Oid collid,
                               OperMinMaxFunc &func,
                               StrategyNumber strategynum);

// Get the operator from pg
bool GetStrategyProcinfo(Oid typid, Oid subtype, Oid *opfamily,
                         std::pair<FmgrInfo, FmgrInfo> &finfos);

// Get the operator from pax
bool GetStrategyProcinfo(Oid typid, Oid subtype,
                         std::pair<OperMinMaxFunc, OperMinMaxFunc> &funcs);

bool CollateIsSupport(Oid collid);
}  // namespace pax