//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CHashSet.inl
//
//	@doc:
//		Inline implementation of hash set template
//
//	@owner: 
//		solimm1
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CHashSet_INL
#define GPOS_CHashSet_INL

#include "gpos/common/clibwrapper.h"

namespace gpos
{	

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::CHashSetElem::CHashSetElem
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
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
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::CHashSetElem::~CHashSetElem
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
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
	
		
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::CHashSet
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
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
		m_ulEntries(0)
	{
		GPOS_ASSERT(ulSize > 0);
		
		m_ppdrgchain = GPOS_NEW_ARRAY(m_pmp, DrgHashChain*, m_ulSize);
		(void) clib::PvMemSet(m_ppdrgchain, 0, m_ulSize * sizeof(DrgHashChain*));
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::~CHashSet
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template <class T, 
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::~CHashSet()
	{
		// release all hash chains
		Clear();
		
		GPOS_DELETE_ARRAY(m_ppdrgchain);
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::Clear
	//
	//	@doc:
	//		Destroy all hash chains; delete elements as per destroy function
	//
	//---------------------------------------------------------------------------
	template <class T, 
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	void
	CHashSet<T, pfnHash, pfnEq, pfnDestroy>::Clear()
	{
		for (ULONG ul = 0; ul < m_ulSize; ul++)
		{
			// release each hash chain
			CRefCount::SafeRelease(m_ppdrgchain[ul]);
		}
		m_ulEntries = 0;
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
		}
		
		CHashSetElem *phse = GPOS_NEW(m_pmp) CHashSetElem(pt, true /*fOwn*/);
		(*ppdrgchain)->Append(phse);
		
		m_ulEntries++;
		return true;
	}
	

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet::FExists
	//
	//	@doc:
	//		Look up element by given key
	//
	//---------------------------------------------------------------------------
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
}


#endif // !GPOS_CHashSet_INL

// EOF
