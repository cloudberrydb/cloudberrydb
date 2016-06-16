//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CHashMapIter.h
//
//	@doc:
//		Hash map iterator
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMapIter_H
#define GPOS_CHashMapIter_H

#include "gpos/base.h"
#include "gpos/common/CStackObject.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMapIter
	//
	//	@doc:
	//		Hash map iterator
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	class CHashMapIter : public CStackObject
	{
	
		// short hand for hashmap type
		typedef CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> TMap;
	
		private:

			// map to iterate
			const TMap *m_ptm;

			// current hashchain
			ULONG m_ulChain;

			// current cursor
			ULONG m_ulElement;
			
			// is initialized?
			BOOL m_fInit;

			// private copy ctor
			CHashMapIter(const CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> &);
			
			// method to return the current element
			const typename TMap::CHashMapElem *Phme() const;

		public:
		
			// ctor
			CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> (TMap *);

			// dtor
			virtual
			~CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> ()
			{}

			// advance iterator to next element
			BOOL FAdvance();
			
			// current key
			const K *Pk() const;

			// current value
			const T *Pt() const;

	}; // class CHashMapIter

}

// inline'd functions
#include "CHashMapIter.inl"

#endif // !GPOS_CHashMapIter_H

// EOF

