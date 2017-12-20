//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.

#include "gpopt/base/CWindowOids.h"

using namespace gpopt;

CWindowOids::CWindowOids(OID oidRowNumber, OID oidRank) {
    m_oidRowNumber = oidRowNumber;
    m_oidRank = oidRank;
}

OID CWindowOids::OidRowNumber() const {
    return m_oidRowNumber;
}

OID CWindowOids::OidRank() const {
    return m_oidRank;
}

CWindowOids *CWindowOids::Pwindowoids(IMemoryPool *pmp) {
    return GPOS_NEW(pmp) CWindowOids(DUMMY_ROW_NUMBER_OID, DUMMY_WIN_RANK);
}
