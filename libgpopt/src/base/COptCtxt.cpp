//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		COptCtxt.cpp
//
//	@doc:
//		Implementation of optimizer context
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/sync/CAutoMutex.h"

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/cost/ICostModel.h"
#include "gpopt/eval/IConstExprEvaluator.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;

// value of the first value part id
ULONG COptCtxt::m_ulFirstValidPartId = 1;

//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::COptCtxt
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
COptCtxt::COptCtxt
	(
	IMemoryPool *pmp,
	CColumnFactory *pcf,
	CMDAccessor *pmda,
	IConstExprEvaluator *pceeval,
	COptimizerConfig *poconf
	)
	:
	CTaskLocalStorageObject(CTaskLocalStorage::EtlsidxOptCtxt),
	m_pmp(pmp),
	m_pcf(pcf),
	m_pmda(pmda),
	m_pceeval(pceeval),
	m_pcomp(GPOS_NEW(m_pmp) CDefaultComparator(pceeval)),
	m_auPartId(m_ulFirstValidPartId),
	m_pcteinfo(NULL),
	m_pdrgpcrSystemCols(NULL),
	m_poconf(poconf),
	m_fDMLQuery(false)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pcf);
	GPOS_ASSERT(NULL != pmda);
	GPOS_ASSERT(NULL != pceeval);
	GPOS_ASSERT(NULL != m_pcomp);
	GPOS_ASSERT(NULL != poconf);
	GPOS_ASSERT(NULL != poconf->Pcm());
	
	m_pcteinfo = GPOS_NEW(m_pmp) CCTEInfo(m_pmp);
	m_pcm = poconf->Pcm();
}


//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::~COptCtxt
//
//	@doc:
//		dtor
//		Does not de-allocate memory pool!
//
//---------------------------------------------------------------------------
COptCtxt::~COptCtxt()
{
	GPOS_DELETE(m_pcf);
	GPOS_DELETE(m_pcomp);
	m_pceeval->Release();
	m_pcteinfo->Release();
	m_poconf->Release();
	CRefCount::SafeRelease(m_pdrgpcrSystemCols);
}


//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::PoctxtCreate
//
//	@doc:
//		Factory method for optimizer context
//
//---------------------------------------------------------------------------
COptCtxt *
COptCtxt::PoctxtCreate
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IConstExprEvaluator *pceeval,
	COptimizerConfig *poconf
	)
{
	GPOS_ASSERT(NULL != poconf);

	// CONSIDER:  - 1/5/09; allocate column factory out of given mem pool
	// instead of having it create its own;
	CColumnFactory *pcf = GPOS_NEW(pmp) CColumnFactory;

	COptCtxt *poctxt = NULL;
	{
		// safe handling of column factory; since it owns a pool that would be
		// leaked if below allocation fails
		CAutoP<CColumnFactory> a_pcf;
		a_pcf = pcf;
		a_pcf.Pt()->Initialize();

		poctxt = GPOS_NEW(pmp) COptCtxt(pmp, pcf, pmda, pceeval, poconf);

		// detach safety
		(void) a_pcf.PtReset();
	}
	return poctxt;
}


//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::FAllEnforcersEnabled
//
//	@doc:
//		Return true if all enforcers are enabled
//
//---------------------------------------------------------------------------
BOOL
COptCtxt::FAllEnforcersEnabled()
{
	BOOL fEnforcerDisabled =
		GPOS_FTRACE(EopttraceDisableMotions) ||
		GPOS_FTRACE(EopttraceDisableMotionBroadcast) ||
		GPOS_FTRACE(EopttraceDisableMotionGather) ||
		GPOS_FTRACE(EopttraceDisableMotionHashDistribute) ||
		GPOS_FTRACE(EopttraceDisableMotionRandom) ||
		GPOS_FTRACE(EopttraceDisableMotionRountedDistribute) ||
		GPOS_FTRACE(EopttraceDisableSort) ||
		GPOS_FTRACE(EopttraceDisableSpool) ||
		GPOS_FTRACE(EopttraceDisablePartPropagation);

	return !fEnforcerDisabled;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		COptCtxt::OsPrint
//
//	@doc:
//		debug print -- necessary to override abstract function in base class
//
//---------------------------------------------------------------------------
IOstream &
COptCtxt::OsPrint
	(
	IOstream &os
	)
	const
{
	// NOOP
	return os;
}

#endif // GPOS_DEBUG

// EOF

