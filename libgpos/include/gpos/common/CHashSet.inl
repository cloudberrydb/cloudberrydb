//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//  Inline implementation of hash set template

#ifndef GPOS_CHashSet_INL
#define GPOS_CHashSet_INL

#include "gpos/common/clibwrapper.h"

namespace gpos
{	

	// ctor
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::CHashSetElem::CHashSetElem
		(
		T *pt,
		BOOL fOwn
		)
		:
		m_pt(pt),
		m_fOwn(fOwn)
	{
		GPOS_ASSERT(NULL != pt);
	}
	
	
	// dtor
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::CHashSetElem::~CHashSetElem()
	{
		// in case of a temporary HashSet element for lookup we do NOT own the
		// objects, otherwise call destroy functions
		if (m_fOwn)
		{
			pfnDestroy(m_pt);
		}
	}
	
		
	// ctor
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::CHashSet
		(
		IMemoryPool *pmp,
		ULONG ulSize
		)
		: 
		m_pmp(pmp), 
		m_ulSize(ulSize),
		m_ulEntries(0),
		m_ppdrgchain(GPOS_NEW_ARRAY(m_pmp, DrgHashChain*, m_ulSize)),
		m_pdrgElements(GPOS_NEW(m_pmp) DrgElements(m_pmp)),
		m_pdrgPiFilledBuckets(GPOS_NEW(pmp) DrgPi(pmp))
	{
		GPOS_ASSERT(ulSize > 0);
		
		(void) clib::PvMemSet(m_ppdrgchain, 0, m_ulSize * sizeof(DrgHashChain*));
	}


	// dtor
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::~CHashSet()
	{
		// release all hash chains
		Clear();
		
		GPOS_DELETE_ARRAY(m_ppdrgchain);
		m_pdrgElements->Release();
		m_pdrgPiFilledBuckets->Release();
	}


	// Destroy all hash chains; delete elements as per destroy function
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	void
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::Clear()
	{
		for (ULONG i = 0; i < m_pdrgPiFilledBuckets->UlLength(); i++)
		{
			// release each hash chain
			m_ppdrgchain[*(*m_pdrgPiFilledBuckets)[i]]->Release();
		}
		m_ulEntries = 0;
		m_pdrgPiFilledBuckets->Clear();
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::FInsert
	//
	//	@doc:
	//		Insert new element and take ownership; returns false if object is
	//		already present
	//
	//---------------------------------------------------------------------------
	template <class T, 
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	BOOL
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::FInsert
		(
		T *pt
		)
	{
		if (FExists(pt))
		{
			return false;
		}

		DrgHashChain **ppdrgchain = PpdrgChain(pt);
		if (NULL == *ppdrgchain)
		{
			*ppdrgchain = GPOS_NEW(m_pmp) DrgHashChain(m_pmp);
			INT iBucket = pfnHash(pt) % m_ulSize;
			m_pdrgPiFilledBuckets->Append(GPOS_NEW(m_pmp) INT(iBucket));
		}
		
		CHashSetElem *phse = GPOS_NEW(m_pmp) CHashSetElem(pt, true /*fOwn*/);
		(*ppdrgchain)->Append(phse);
		
		m_ulEntries++;
		m_pdrgElements->Append(pt);

		return true;
	}
	

	// Look up element by given element
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	BOOL
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::FExists
		(
		const T *pt
		)
		const
	{
		CHashSetElem hse(const_cast<T*>(pt), false /*fOwn*/);
		DrgHashChain **ppdrgchain = PpdrgChain(pt);
		if (NULL != *ppdrgchain)
		{
			CHashSetElem *phse = (*ppdrgchain)->PtLookup(&hse);

			return (NULL != phse);					
		}
		
		return false;
	}

	// Look up element
	template <class T,
	ULONG (*pfnHash)(const T*),
	BOOL (*pfnEq)(const T*, const T*),
	void (*pfnDestroy)(T*)>
	void
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::Lookup
	(
	const T *pt,
	CHashSetElem **pphse // output : pointer to found set entry
	)
	const
	{
		GPOS_ASSERT(NULL != pphse);

		CHashSetElem hse(const_cast<T*>(pt), false /*fOwn*/);
		CHashSetElem *phse = NULL;
		DrgHashChain **ppdrgchain = PpdrgChain(pt);
		if (NULL != *ppdrgchain)
		{
			phse = (*ppdrgchain)->PtLookup(&hse);
			GPOS_ASSERT_IMP(NULL != phse, *phse == hse);
		}

		*pphse = phse;
	}

}


#endif // !GPOS_CHashSet_INL

// EOF
