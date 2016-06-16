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
		m_ulElement((ULONG)-1)
	{
		GPOS_ASSERT(NULL != ptm);
	}


	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter::FAdvance
	//
	//	@doc:
	//		Advance cursor; increment element position -- if not existent, try
	//		to find next existing hash chain;
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
		// bump counter
		m_ulElement++;

		// if position is not valid, find next valid element
		while (m_ulChain < m_ptm->m_ulSize)
		{
			typename TMap::DrgHashChain *pdrgchain;
			pdrgchain = m_ptm->m_ppdrgchain[m_ulChain];
			
			if (NULL != pdrgchain && m_ulElement < pdrgchain->UlLength())
			{
				return true;
			}

			m_ulElement = 0;
			m_ulChain++;
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
		typename TMap::DrgHashChain *pdrgchain;
		pdrgchain = m_ptm->m_ppdrgchain[m_ulChain];
		GPOS_ASSERT(NULL != pdrgchain);
			
		if (m_ulElement < pdrgchain->UlLength())
		{
			return (*pdrgchain)[m_ulElement];
		}
		
		return NULL;
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
