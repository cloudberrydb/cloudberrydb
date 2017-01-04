//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CAutoMemoryPool.cpp
//
//	@doc:
//		Implementation for auto memory pool that automatically releases
//  	the attached memory pool and performs leak checking
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/error/CErrorHandler.h"
#include "gpos/task/ITask.h"
#include "gpos/task/CAutoSuspendAbort.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CAutoMemoryPool::CAutoMemoryPool
//
//	@doc:
//		Create an auto-managed pool; the managed pool is allocated from
//  	the CMemoryPoolManager global instance
//
//---------------------------------------------------------------------------
CAutoMemoryPool::CAutoMemoryPool
	(
	ELeakCheck elc,
	CMemoryPoolManager::EAllocType ept,
	BOOL fThreadSafe,
	ULLONG ullCapacity
	)
	:
	m_elc(elc)
{
	m_pmp = CMemoryPoolManager::Pmpm()->PmpCreate(ept, fThreadSafe, ullCapacity);
}



//---------------------------------------------------------------------------
//	@function:
//		CAutoMemoryPool::PmpDetach
//
//	@doc:
//		Detach function used when CAutoMemoryPool is used to guard a newly
//		created pool until it is safe, e.g., in constructors
//
//---------------------------------------------------------------------------
IMemoryPool *
CAutoMemoryPool::PmpDetach()
{
	IMemoryPool *pmp = m_pmp;
	m_pmp = NULL;
	
	return pmp;
}


//---------------------------------------------------------------------------
//	@function:
//		CAutoMemoryPool::~CAutoMemoryPool
//
//	@doc:
//		Release the pool back to the manager and perform leak checks if
//		(1) strict leak checking indicated, or
//		(2) no checking while pending exception indicated and no pending exception
//
//---------------------------------------------------------------------------
CAutoMemoryPool::~CAutoMemoryPool()
{
	if (NULL == m_pmp)
	{
		return;
	}
	
	// suspend cancellation
	CAutoSuspendAbort asa;

#ifdef GPOS_DEBUG

	ITask *ptsk = ITask::PtskSelf();
	
	// ElcExc must be used inside tasks only
	GPOS_ASSERT_IMP(ElcExc == m_elc, NULL != ptsk);
	
	GPOS_TRY
	{
		if (ElcStrict == m_elc || (ElcExc == m_elc && !ptsk->Perrctxt()->FPending()))
		{
			gpos::IOstream &os = gpos::oswcerr;

			// check for leaks, use this to trigger standard Assert handling
			m_pmp->AssertEmpty(os);
		}

		// release pool
		CMemoryPoolManager::Pmpm()->Destroy(m_pmp);
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_ASSERT(GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiAssert));

		// release pool
		CMemoryPoolManager::Pmpm()->Destroy(m_pmp);	
		
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

#else // GPOS_DEBUG
	
	// hand in pool and return
	CMemoryPoolManager::Pmpm()->Destroy(m_pmp);

#endif // GPOS_DEBUG
}

// EOF

