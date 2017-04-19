//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc
//
//  Inline implementation of hash set iterator template

#ifndef GPOS_CHashSetIter_INL
#define GPOS_CHashSetIter_INL

namespace gpos
{
	// ctor
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	CHashSetIter<T, pfnHash, pfnEq, pfnDestroy>::CHashSetIter
		(
		TSet *pts
		)
		: 
		m_pts(pts),
		m_ulChain(0),
		m_ulElement(0)
	{
		GPOS_ASSERT(NULL != pts);
	}


	// Get the next existent hash chain
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	BOOL
	CHashSetIter<T, pfnHash, pfnEq, pfnDestroy>::FAdvance()
	{
		if (m_ulElement < m_pts->m_pdrgElements->UlLength())
		{
			m_ulElement++;
			return true;
		}

		return false;
	}

	// Look up current element
	template <class T,
				ULONG (*pfnHash)(const T*),
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	const typename CHashSet<T, pfnHash, pfnEq, pfnDestroy>::CHashSetElem *
	CHashSetIter<T, pfnHash, pfnEq, pfnDestroy>::Phse() const
	{
		typename TSet::CHashSetElem *phse = NULL;
		T *t = (*(m_pts->m_pdrgElements))[m_ulElement-1];
		m_pts->Lookup(t, &phse);

		return phse;
	}


	// Look up current element
	template <class T,
				ULONG (*pfnHash)(const T*),
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	const T*
	CHashSetIter<T, pfnHash, pfnEq, pfnDestroy>::Pt() const
	{
		const typename TSet::CHashSetElem *phse = Phse();
		if (NULL != phse)
		{
			return phse->Pt();
		}
		return NULL;
	}
}


#endif // !GPOS_CHashSetIter_INL

// EOF
