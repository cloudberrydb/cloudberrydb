//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CFSimulator.cpp
//
//	@doc:
//		Failpoint simulator; maintains a hashtable of bitvectors which encode
//		stacks. Stack walker determines computes a hash value for call stack
//		when checking for a failpoint: if stack has been seen before, skip, 
//		otherwise raise exception.
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CFSimulator.h"

#ifdef GPOS_FPSIMULATOR

#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/common/CAutoP.h"
#include "gpos/task/CAutoTraceFlag.h"

using namespace gpos;

// global instance of simulator
CFSimulator *CFSimulator::m_pfsim = NULL;


// invalid stack key
const CFSimulator::CStackTracker::SStackKey
	CFSimulator::CStackTracker::m_skeyInvalid
		(
		CException::ExmaInvalid,
		CException::ExmiInvalid
		);

//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::AddTracker
//
//	@doc:
//		Attempt inserting a new tracker for a given exception; we can get
//		overtaken while allocating a tracker before inserting it; in this case
//		back out and delete tracker;
//
//---------------------------------------------------------------------------
void
CFSimulator::AddTracker
	(
	CStackTracker::SStackKey skey
	)
{
	// disable OOM simulation in this scope
	CAutoTraceFlag atf(EtraceSimulateOOM, false);

	// allocate new tracker before getting the spinlock
	CStackTracker *pstrackNew = GPOS_NEW(m_pmp) CStackTracker(m_pmp, m_cResolution, skey);
	
	// assume somebody overtook
	BOOL fOvertaken = true;
	
	// scope for accessor
	{
		CStackTableAccessor stacc(m_st, skey);
		CStackTracker *pstrack = stacc.PtLookup();
		
		if (NULL == pstrack)
		{
			fOvertaken = false;
			stacc.Insert(pstrackNew);
		}
		
		// must have tracker now
		GPOS_ASSERT(NULL != stacc.PtLookup());
	}
	
	// clean up as necessary
	if (fOvertaken)
	{
		GPOS_DELETE(pstrackNew);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::FNewStack
//
//	@doc:
//		Determine if stack is unknown so far and, if so, add to repository
//
//---------------------------------------------------------------------------
BOOL
CFSimulator::FNewStack
	(
	ULONG ulMajor,
	ULONG ulMinor
	)
{
	// hash stack
	CStackDescriptor m_sd;
	m_sd.BackTrace();
	ULONG ulHash = m_sd.UlHash();
	
	CStackTracker::SStackKey skey(ulMajor, ulMinor);
	
	// attempt direct lookup; if we don't have a tracker yet, we 
	// need to retry exactly once
	for (ULONG i = 0; i < 2; i++)
	{
		// scope for hashtable access
		{
			CStackTableAccessor stacc(m_st, skey);
			CStackTracker *pstrack = stacc.PtLookup();
			
			// always true once a tracker has been initialized
			if (NULL != pstrack)
			{
				return false == pstrack->FExchangeSet(ulHash % m_cResolution);
			}
		}
				
		// very first time we call in here: fall through and add new tracker
		this->AddTracker(skey);
	}
	
	GPOS_ASSERT(!"Unexpected exit from loop");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::Init
//
//	@doc:
//		Initialize global instance
//
//---------------------------------------------------------------------------
GPOS_RESULT
CFSimulator::EresInit()
{
	
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	CFSimulator::m_pfsim = GPOS_NEW(pmp) CFSimulator(pmp, GPOS_FSIM_RESOLUTION);

	// detach safety
	(void) amp.PmpDetach();
	
	return GPOS_OK;
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::Shutdown
//
//	@doc:
//		Destroy singleton instance
//
//---------------------------------------------------------------------------
void
CFSimulator::Shutdown()
{
	IMemoryPool *pmp = m_pmp;
	GPOS_DELETE(CFSimulator::m_pfsim);
	CFSimulator::m_pfsim = NULL;
	
	CMemoryPoolManager::Pmpm()->Destroy(pmp);
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::CStackTracker::CStackTracker
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CFSimulator::CStackTracker::CStackTracker
	(
	IMemoryPool *pmp,
	ULONG cResolution, 
	SStackKey skey
	)
	:
	m_skey(skey),
	m_pbv(NULL)
{
	// allocate bit vector
	m_pbv = GPOS_NEW(pmp) CBitVector(pmp, cResolution);
}


//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::CStackTracker::FExchangeSet
//
//	@doc:
//		Test and set a given bit in the bit vector
//
//---------------------------------------------------------------------------
BOOL
CFSimulator::CStackTracker::FExchangeSet
	(
	ULONG ulBit
	)
{
	return m_pbv->FExchangeSet(ulBit);
}


//---------------------------------------------------------------------------
//	@function:
//		CFSimulator::CFSimulator
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CFSimulator::CFSimulator
	(
	IMemoryPool *pmp,
	ULONG cResolution
	)
	:
	m_pmp(pmp),
	m_cResolution(cResolution)
{
	// setup init table
	m_st.Init
		(
		m_pmp,
		1024,
		GPOS_OFFSET(CStackTracker, m_link),
		GPOS_OFFSET(CStackTracker, m_skey),
		&(CStackTracker::m_skeyInvalid),
		CStackTracker::SStackKey::UlHash,
		CStackTracker::SStackKey::FEqual
		);
}

#endif // GPOS_FPSIMULATOR

// EOF

