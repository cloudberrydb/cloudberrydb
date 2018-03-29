//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCacheFactory.cpp
//
//	@doc:
//		 Function implementation of CCacheFactory
//---------------------------------------------------------------------------


#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCache.h"
#include "gpos/memory/CCacheFactory.h"

using namespace gpos;

// global instance of cache factory
CCacheFactory *CCacheFactory::m_pcf = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::CCacheFactory
//
//	@doc:
//		Ctor;
//
//---------------------------------------------------------------------------
CCacheFactory::CCacheFactory
	(
		IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{

}


//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::Pmp
//
//	@doc:
//		Returns a pointer to allocated memory pool
//
//---------------------------------------------------------------------------
IMemoryPool *
CCacheFactory::Pmp() const
{
	return m_pmp;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::EresInit
//
//	@doc:
//		Initializes global instance
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCacheFactory::EresInit()
{
	GPOS_ASSERT(NULL == Pcf() &&
			    "Cache factory was already initialized");

	GPOS_RESULT eres = GPOS_OK;

	// create cache factory memory pool
    IMemoryPool *pmp = CMemoryPoolManager::Pmpm()->PmpCreate(
        CMemoryPoolManager::EatTracker, true /*fThreadSafe*/, gpos::ullong_max);
	GPOS_TRY
	{
		// create cache factory instance
		CCacheFactory::m_pcf = GPOS_NEW(pmp) CCacheFactory(pmp);
	}
	GPOS_CATCH_EX(ex)
	{
		// destroy memory pool if global instance was not created
		CMemoryPoolManager::Pmpm()->Destroy(pmp);

		CCacheFactory::m_pcf = NULL;

		if (GPOS_MATCH_EX(ex, CException::ExmaSystem, CException::ExmiOOM))
		{
			eres = GPOS_OOM;
		}
		else
		{
			eres = GPOS_FAILED;
		}
	}
	GPOS_CATCH_END;
	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheFactory::Shutdown
//
//	@doc:
//		Cleans up allocated memory pool
//
//---------------------------------------------------------------------------
void
CCacheFactory::Shutdown()
{
	CCacheFactory *pcf = CCacheFactory::Pcf();

	GPOS_ASSERT(NULL != pcf &&
			    "Cache factory has not been initialized");

	IMemoryPool *pmp = pcf->m_pmp;

	// destroy cache factory
	CCacheFactory::m_pcf = NULL;
	GPOS_DELETE(pcf);

	// release allocated memory pool
	CMemoryPoolManager::Pmpm()->Destroy(pmp);
}
// EOF
