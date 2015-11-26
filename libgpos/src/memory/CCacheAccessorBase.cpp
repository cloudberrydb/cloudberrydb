//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CCacheAccessorBase.cpp
//
//	@doc:
//		 Function implementation of CCacheAccessorBase
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/memory/CCacheAccessorBase.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::CCacheAccessorBase
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCacheAccessorBase::CCacheAccessorBase
	(
	CCache  *pcache
	)
	:
	m_pcache(pcache),
	m_pmp(NULL),
	m_pce(NULL),
	m_fInserted(false)
{
	GPOS_ASSERT(NULL != pcache);
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::~CCacheAccessorBase
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCacheAccessorBase::~CCacheAccessorBase()
{
	// check if a memory pool was created but insertion failed
	if (NULL != m_pmp && !m_fInserted)
	{
		CMemoryPoolManager::Pmpm()->Destroy(m_pmp);
	}

	// release entry if one was created
	if (NULL != m_pce)
	{
		m_pcache->ReleaseEntry(m_pce);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::Pmp
//
//	@doc:
//		Creates a new memory pool for storing an object to be cached
//
//---------------------------------------------------------------------------
IMemoryPool *
CCacheAccessorBase::Pmp()
{
	GPOS_ASSERT(NULL == m_pmp);

	// construct a memory pool for cache entry
	m_pmp = CMemoryPoolManager::Pmpm()->PmpCreate
			(
			CMemoryPoolManager::EatMalloc,
			true /*fThreadSafe*/,
			ULLONG_MAX
			);

	return m_pmp;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::PvInsert
//
//	@doc:
//		Inserts a new object; returns the value of passed object if insertion
//		succeeds, otherwise returns the first object with a matching key
//
//---------------------------------------------------------------------------
VOID_PTR
CCacheAccessorBase::PvInsert
	(
	VOID_PTR pvKey,
	VOID_PTR pvVal
	)
{
	GPOS_ASSERT(NULL != m_pmp);

	GPOS_ASSERT(!m_fInserted &&
			    "Accessor was already used for insertion");

	GPOS_ASSERT(NULL == m_pce &&
			    "Accessor already holds an entry");

	CCache::CCacheEntry *pce =
			New(m_pcache->m_pmp) CCache::CCacheEntry(m_pmp, pvKey, pvVal, m_pcache->m_ulGClockInitCounter);
	CCache::CCacheEntry *pceReturn = m_pcache->PceInsert(pce);

	// check if insertion completed successfully
	if (pce == pceReturn)
	{
		m_fInserted = true;
	}
	else
	{
		GPOS_DELETE(pce);
	}

	// accessor holds the returned entry in all cases
	m_pce = pceReturn;

	return pceReturn->PvVal();
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::Lookup
//
//	@doc:
//		Finds the first cached object matching the given key.
//
//---------------------------------------------------------------------------
void
CCacheAccessorBase::Lookup
	(
	VOID_PTR pvKey
	)
{
	GPOS_ASSERT(NULL == m_pce && "Accessor already holds an entry");

	m_pce = m_pcache->PceLookup(pvKey);
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::PvVal
//
//	@doc:
//		Returns the current object held by the accessor
//
//---------------------------------------------------------------------------
VOID_PTR
CCacheAccessorBase::PvVal() const
{
	VOID_PTR pvReturn = NULL;
	if (NULL != m_pce)
	{
		pvReturn = m_pce->PvVal();
	}

	return pvReturn;
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::PvNext
//
//	@doc:
//		Returns the first object following current object, in the hash chain,
//		and having the same key.
//
//---------------------------------------------------------------------------
VOID_PTR
CCacheAccessorBase::PvNext()
{
	GPOS_ASSERT(NULL != m_pce);

	CCache::CCacheEntry *pce = m_pce;
	m_pce = m_pcache->PceNext(m_pce);

	// release previous entry
	m_pcache->ReleaseEntry(pce);

	return PvVal();
}


//---------------------------------------------------------------------------
//	@function:
//		CCacheAccessorBase::MarkForDeletion
//
//	@doc:
//		Marks currently held object as deleted
//
//---------------------------------------------------------------------------
void
CCacheAccessorBase::MarkForDeletion()
{
	GPOS_ASSERT(NULL != m_pce);

	m_pce->MarkForDeletion();
}

// EOF
