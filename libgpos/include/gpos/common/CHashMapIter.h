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

			// current key
			ULONG m_ulKey;

			// is initialized?
			BOOL m_fInit;

			// private copy ctor
			CHashMapIter(const CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> &);
			
			// method to return the current element
			const typename TMap::CHashMapElem *Phme() const
            {
                typename TMap::CHashMapElem *phme = NULL;
                K *k = (*(m_ptm->m_pdrgKeys))[m_ulKey-1];
                m_ptm->Lookup(k, &phme);

                return phme;
            }

		public:
		
			// ctor
			CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> (TMap *ptm)
            :
            m_ptm(ptm),
            m_ulChain(0),
            m_ulKey(0)
            {
                GPOS_ASSERT(NULL != ptm);
            }

			// dtor
			virtual
			~CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> ()
			{}

			// advance iterator to next element
			BOOL FAdvance()
            {
                if (m_ulKey < m_ptm->m_pdrgKeys->UlLength())
                {
                    m_ulKey++;
                    return true;
                }

                return false;
            }
			
			// current key
			const K *Pk() const
            {
                const typename TMap::CHashMapElem *phme = Phme();
                if (NULL != phme)
                {
                    return phme->Pk();
                }
                return NULL;
            }

			// current value
			const T *Pt() const
            {
                const typename TMap::CHashMapElem *phme = Phme();
                if (NULL != phme)
                {
                    return phme->Pt();
                }
                return NULL;
            }

	}; // class CHashMapIter

}

#endif // !GPOS_CHashMapIter_H

// EOF

