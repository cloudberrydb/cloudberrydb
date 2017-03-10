//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHashMapIter.inl
//
//	@doc:
//		Inline implementation of hash map iterator template
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMapIter_INL
#define GPOS_CHashMapIter_INL

namespace gpos
{	

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter::CHashMapIter
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
	CHashMapIter<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::CHashMapIter
		(
		TMap *ptm
		)
		: 
		m_ptm(ptm),
		m_ulChain(0),
		m_ulKey(0)
	{
		GPOS_ASSERT(NULL != ptm);
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter::FAdvance
	//
	//	@doc:
	//		Get the next existent hash chain
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	BOOL
	CHashMapIter<K, T,pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::FAdvance()
	{
		if (m_ulKey < m_ptm->m_pdrgKeys->UlLength())
		{
			m_ulKey++;
			return true;
		}

		return false;
	}

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter::Phme
	//
	//	@doc:
	//		Look up current key value pair
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*),
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	const typename CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::CHashMapElem *
	CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::Phme() const
	{
		typename TMap::CHashMapElem *phme = NULL;
		K *k = (*(m_ptm->m_pdrgKeys))[m_ulKey-1];
		m_ptm->Lookup(k, &phme);

		return phme;
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter::Pk
	//
	//	@doc:
	//		Look up current key
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*),
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	const K*
	CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::Pk() const
	{
		const typename TMap::CHashMapElem *phme = Phme();
		if (NULL != phme)
		{
			return phme->Pk();
		}
		return NULL;
	}
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter::Pt
	//
	//	@doc:
	//		Look up current value
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*),
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	const T*
	CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>::Pt() const
	{
		const typename TMap::CHashMapElem *phme = Phme();
		if (NULL != phme)
		{
			return phme->Pt();
		}
		return NULL;
	}
}


#endif // !GPOS_CHashMapIter_INL

// EOF
