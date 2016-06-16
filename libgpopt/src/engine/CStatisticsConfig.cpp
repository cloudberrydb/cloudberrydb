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
	m_phmmdidcolinfo(NULL)
{
	GPOS_ASSERT(CDouble(0.0) < dDampingFactorFilter);
	GPOS_ASSERT(CDouble(0.0) < dDampingFactorJoin);
	GPOS_ASSERT(CDouble(0.0) < dDampingFactorGroupBy);

	//m_phmmdidcolinfo = New(m_pmp) HMMDIdMissingstatscol(m_pmp);
	m_phmmdidcolinfo = GPOS_NEW(m_pmp) HMMDIdMDId(m_pmp);
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
	m_phmmdidcolinfo->Release();
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

	// add the new column information to the hash map
	// to be sure that no one else does this at the same time, lock the mutex
	CAutoMutex am(m_mutexMissingColStats);
	am.Lock();

	if (NULL == m_phmmdidcolinfo->PtLookup(pmdidCol))
	{
		pmdidCol->AddRef();
		pmdidCol->AddRef();
		m_phmmdidcolinfo->FInsert(pmdidCol, pmdidCol);
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

	HMIterMDIdMDId hmiter(m_phmmdidcolinfo);
	while (hmiter.FAdvance())
	{
		CMDIdColStats *pmdidColStats = CMDIdColStats::PmdidConvert(const_cast<IMDId *>(hmiter.Pt()));
		pmdidColStats->AddRef();
		pdrgmdid->Append(pmdidColStats);
	}
}


// EOF

