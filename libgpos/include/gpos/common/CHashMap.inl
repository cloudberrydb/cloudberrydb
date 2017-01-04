//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CHashMap.inl
//
//	@doc:
//		Inline implementation of hash map template
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMap_INL
#define GPOS_CHashMap_INL

#include "gpos/common/clibwrapper.h"

namespace gpos
{	

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::CHashMapElem::CHashMapElem
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::CHashMapElem::CHashMapElem
		(
			K *pk,
			T *pt,
			BOOL fOwn
		)
		:
		m_pk(pk),
		m_pt(pt),
		m_fOwn(fOwn)
	{
		GPOS_ASSERT(NULL != pk);
	}
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::CHashMapElem::~CHashMapElem
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::CHashMapElem::~CHashMapElem()
	{
		// in case of a temporary hashmap element for lookup we do NOT own the
		// objects, otherwise call destroy functions
		if (m_fOwn)
		{
			pfnDestroyK(m_pk);
			pfnDestroyT(m_pt);
		}
	}
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::CHashMapElem::ReplaceValue
	//
	//	@doc:
	//		Replace value
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	void
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::CHashMapElem::ReplaceValue
		(
		T *ptNew
		)
	{
		if (m_fOwn)
		{
			pfnDestroyT(m_pt);
		}
		m_pt = ptNew;
	}
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::CHashMap
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::CHashMap
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
	//		CHashMap::~CHashMap
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::~CHashMap()
	{
		// release all hash chains
		Clear();
		
		GPOS_DELETE_ARRAY(m_ppdrgchain);
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::Clear
	//
	//	@doc:
	//		Destroy all hash chains; delete elements as per destroy function
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	void
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::Clear()
	{
		for (ULONG i = 0; i < m_ulSize; i++)
		{
			// release each hash chain
			CRefCount::SafeRelease(m_ppdrgchain[i]);
		}
		m_ulEntries = 0;
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::FInsert
	//
	//	@doc:
	//		Insert new element and take ownership; returns false if object is
	//		already present
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	BOOL
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::FInsert
		(
		K *pk, 
		T *pt
		)
	{
		if (NULL != PtLookup(pk))
		{
			return false;
		}

		DrgHashChain **ppdrgchain = PpdrgChain(pk);
		if (NULL == *ppdrgchain)
		{
			*ppdrgchain = GPOS_NEW(m_pmp) DrgHashChain(m_pmp);
		}
		
		CHashMapElem *phme = GPOS_NEW(m_pmp) CHashMapElem(pk, pt, true /*fOwn*/);
		(*ppdrgchain)->Append(phme);
		
		m_ulEntries++;
		return true;
	}
	

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::Lookup
	//
	//	@doc:
	//		Look up element by given key
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*),
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	void
	CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::Lookup
		(
		const K *pk,
		CHashMapElem **pphme // output : pointer to found map entry
		)
		const
	{
		GPOS_ASSERT(NULL != pphme);
		
		CHashMapElem hme(const_cast<K*>(pk), NULL /*T*/, false /*fOwn*/);
		CHashMapElem *phme = NULL;
		DrgHashChain **ppdrgchain = PpdrgChain(pk);
		if (NULL != *ppdrgchain)
		{
			phme = (*ppdrgchain)->PtLookup(&hme);
			GPOS_ASSERT_IMP(NULL != phme, *phme == hme);					
		}
		
		*pphme = phme;
	}

		
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::PtLookup
	//
	//	@doc:
	//		Look up value by given key
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*),
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	T*
	CHashMap<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::PtLookup
		(
		const K *pk
		)
		const
	{
		CHashMapElem *phme = NULL;
		Lookup(pk, &phme);
		if (NULL != phme)
		{
			return phme->Pt();
		}
		
		return NULL;
	}
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap::FReplace
	//
	//	@doc:
	//		Replace the value of a map entry with the given value;
	//		return TRUE if the required map entry is found and value replacement
	//		succeeded, otherwise return FALSE
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	BOOL
	CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::FReplace
		(
		const K *pk,
		T *ptNew
		)
	{
		GPOS_ASSERT(NULL != pk);
		
		BOOL fSuccess = false;
		CHashMapElem *phme = NULL;
		Lookup(pk, &phme);
		if (NULL != phme)
		{
			phme->ReplaceValue(ptNew);
			fSuccess = true;
		}
		
		return fSuccess;
	}
}


#endif // !GPOS_CHashMap_INL

// EOF
