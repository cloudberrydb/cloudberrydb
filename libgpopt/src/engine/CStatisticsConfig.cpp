//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CStatisticsConfig.cpp
//
//	@doc:
//		Implementation of statistics context
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CHashMapIter.h"
#include "gpos/sync/CAutoMutex.h"

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/engine/CStatisticsConfig.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CStatisticsConfig::CStatisticsConfig
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CStatisticsConfig::CStatisticsConfig
	(
	IMemoryPool *pmp,
	CDouble dDampingFactorFilter,
	CDouble dDampingFactorJoin,
	CDouble dDampingFactorGroupBy
	)
	:
	m_pmp(pmp),
	m_dDampingFactorFilter(dDampingFactorFilter),
	m_dDampingFactorJoin(dDampingFactorJoin),
	m_dDampingFactorGroupBy(dDampingFactorGroupBy),
	m_phsmdidcolinfo(NULL)
{
	GPOS_ASSERT(CDouble(0.0) < dDampingFactorFilter);
	GPOS_ASSERT(CDouble(0.0) < dDampingFactorJoin);
	GPOS_ASSERT(CDouble(0.0) < dDampingFactorGroupBy);

	//m_phmmdidcolinfo = New(m_pmp) HMMDIdMissingstatscol(m_pmp);
	m_phsmdidcolinfo = GPOS_NEW(m_pmp) HSMDId(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatisticsConfig::~CStatisticsConfig
//
//	@doc:
//		dtor
//		Does not de-allocate memory pool!
//
//---------------------------------------------------------------------------
CStatisticsConfig::~CStatisticsConfig()
{
	m_phsmdidcolinfo->Release();
}

//---------------------------------------------------------------------------
//      @function:
//              CStatisticsConfig::AddMissingStatsColumn
//
//      @doc:
//              Add the information about the column with the missing statistics
//
//---------------------------------------------------------------------------
void
CStatisticsConfig::AddMissingStatsColumn
	(
	CMDIdColStats *pmdidCol
	)
{
	GPOS_ASSERT(NULL != pmdidCol);

	// add the new column information to the hash set
	// to be sure that no one else does this at the same time, lock the mutex
	CAutoMutex am(m_mutexMissingColStats);
	am.Lock();

	if (m_phsmdidcolinfo->FInsert(pmdidCol))
	{
		pmdidCol->AddRef();
	}
}


//---------------------------------------------------------------------------
//      @function:
//              CStatisticsConfig::CollectMissingColumns
//
//      @doc:
//              Collect the columns with missing stats
//
//---------------------------------------------------------------------------
void
CStatisticsConfig::CollectMissingStatsColumns
	(
	DrgPmdid *pdrgmdid
    )
{
	GPOS_ASSERT(NULL != pdrgmdid);

	CAutoMutex am(m_mutexMissingColStats);
	am.Lock();

	HSIterMDId hsiter(m_phsmdidcolinfo);
	while (hsiter.FAdvance())
	{
		CMDIdColStats *pmdidColStats = CMDIdColStats::PmdidConvert(const_cast<IMDId *>(hsiter.Pt()));
		pmdidColStats->AddRef();
		pdrgmdid->Append(pmdidColStats);
	}
}


// EOF

