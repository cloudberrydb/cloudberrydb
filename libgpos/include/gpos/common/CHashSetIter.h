//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc
//
//	Hash set iterator

#ifndef GPOS_CHashSetIter_H
#define GPOS_CHashSetIter_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"
#include "gpos/common/CHashSet.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{	

	// Hash set iterator
	template <class T,
				ULONG (*pfnHash)(const T*), 
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	class CHashSetIter : public CStackObject
	{
	
		// short hand for hashset type
		typedef CHashSet<T, pfnHash, pfnEq, pfnDestroy> TSet;
	
		private:

			// set to iterate
			const TSet *m_pts;

			// current hashchain
			ULONG m_ulChain;

			// current element
			ULONG m_ulElement;

			// is initialized?
			BOOL m_fInit;

			// private copy ctor
			CHashSetIter(const CHashSetIter<T, pfnHash, pfnEq, pfnDestroy> &);
			
			// method to return the current element
			const typename TSet::CHashSetElem *Phse() const;

		public:
		
			// ctor
			CHashSetIter<T, pfnHash, pfnEq, pfnDestroy> (TSet *);

			// dtor
			virtual
			~CHashSetIter<T, pfnHash, pfnEq, pfnDestroy> ()
			{}

			// advance iterator to next element
			BOOL FAdvance();
			
			// current element
			const T *Pt() const;

	}; // class CHashSetIter

}

// inline'd functions
#include "CHashSetIter.inl"

#endif // !GPOS_CHashSetIter_H

// EOF

