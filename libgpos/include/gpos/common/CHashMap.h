//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CHashMap.h
//
//	@doc:
//		Hash map
//		* stores deep objects, i.e., pointers
//		* equality == on key uses template function argument
//		* does not allow insertion of duplicates (no equality on value class req'd)
//		* destroys objects based on client-side provided destroy functions
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMap_H
#define GPOS_CHashMap_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{	
	// fwd declaration
	template <class K, class T, 
		ULONG (*pfnHash)(const K*), 
		BOOL (*pfnEq)(const K*, const K*),
		void (*pfnDestroyK)(K*),
		void (*pfnDestroyT)(T*)>
	class CHashMapIter;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap
	//
	//	@doc:
	//		Hash map
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	class CHashMap : public CRefCount
	{
		// fwd declaration
		friend class CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>;

		private:
		
			//---------------------------------------------------------------------------
			//	@class:
			//		CHashMapElem
			//
			//	@doc:
			//		Anchor for key/value pair
			//
			//---------------------------------------------------------------------------		
			class CHashMapElem
			{
				private:
				
					// key/value pair
					K *m_pk;
					T *m_pt;
					
					// own objects
					BOOL m_fOwn;
					
					// private copy ctor
					CHashMapElem(const CHashMapElem &);
				
				public:
				
					// ctor
					CHashMapElem(K *pk, T *pt, BOOL fOwn)
                    :
                    m_pk(pk),
                    m_pt(pt),
                    m_fOwn(fOwn)
                    {
                        GPOS_ASSERT(NULL != pk);
                    }

					// dtor 
					~CHashMapElem()
                    {
                        // in case of a temporary hashmap element for lookup we do NOT own the
                        // objects, otherwise call destroy functions
                        if (m_fOwn)
                        {
                            pfnDestroyK(m_pk);
                            pfnDestroyT(m_pt);
                        }
                    }

					// key accessor
					K *Pk() const
					{
						return m_pk;
					}

					// value accessor
					T *Pt() const
					{
						return m_pt;
					}
					
					// replace value
					void ReplaceValue(T *ptNew)
                    {
                        if (m_fOwn)
                        {
                            pfnDestroyT(m_pt);
                        }
                        m_pt = ptNew;
                    }

					// equality operator -- map elements are equal if their keys match
					BOOL operator == (const CHashMapElem &hme) const
					{
						return pfnEq(m_pk, hme.m_pk);
					}
			};

			// memory pool
			IMemoryPool *const m_pmp;
			
			// size
			ULONG m_ulSize;
		
			// number of entries
			ULONG m_ulEntries;

			// each hash chain is an array of hashmap elements
			typedef CDynamicPtrArray<CHashMapElem, CleanupDelete> DrgHashChain;
			DrgHashChain **const m_ppdrgchain;

			// array for keys
			// We use CleanupNULL because the keys are owned by the hash table
			typedef CDynamicPtrArray<K, CleanupNULL> DrgKeys;
			DrgKeys *const m_pdrgKeys;

			DrgPi *const m_pdrgPiFilledBuckets;

			// private copy ctor
			CHashMap(const CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> &);
			
			// lookup appropriate hash chain in static table, may be NULL if
			// no elements have been inserted yet
			DrgHashChain **PpdrgChain(const K *pk) const
			{
				GPOS_ASSERT(NULL != m_ppdrgchain);
				return &m_ppdrgchain[pfnHash(pk) % m_ulSize];
			}

			// clear elements
			void Clear()
            {
                for (ULONG i = 0; i < m_pdrgPiFilledBuckets->UlLength(); i++)
                {
                    // release each hash chain
                    m_ppdrgchain[*(*m_pdrgPiFilledBuckets)[i]]->Release();
                }
                m_ulEntries = 0;
                m_pdrgPiFilledBuckets->Clear();
            }
	
			// lookup an element by its key
			void Lookup(const K *pk, CHashMapElem **pphme) const
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

		public:
		
			// ctor
			CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> (IMemoryPool *pmp, ULONG ulSize = 128)
            :
            m_pmp(pmp),
            m_ulSize(ulSize),
            m_ulEntries(0),
            m_ppdrgchain(GPOS_NEW_ARRAY(m_pmp, DrgHashChain*, m_ulSize)),
            m_pdrgKeys(GPOS_NEW(m_pmp) DrgKeys(m_pmp)),
            m_pdrgPiFilledBuckets(GPOS_NEW(pmp) DrgPi(pmp))
            {
                GPOS_ASSERT(ulSize > 0);
                (void) clib::PvMemSet(m_ppdrgchain, 0, m_ulSize * sizeof(DrgHashChain*));
            }

			// dtor
			~CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> ()
            {
                // release all hash chains
                Clear();

                GPOS_DELETE_ARRAY(m_ppdrgchain);
                m_pdrgKeys->Release();
                m_pdrgPiFilledBuckets->Release();
            }

			// insert an element if key is not yet present
			BOOL FInsert(K *pk, T *pt)
            {
                if (NULL != PtLookup(pk))
                {
                    return false;
                }

                DrgHashChain **ppdrgchain = PpdrgChain(pk);
                if (NULL == *ppdrgchain)
                {
                    *ppdrgchain = GPOS_NEW(m_pmp) DrgHashChain(m_pmp);
                    INT iBucket = pfnHash(pk) % m_ulSize;
                    m_pdrgPiFilledBuckets->Append(GPOS_NEW(m_pmp) INT(iBucket));
                }

                CHashMapElem *phme = GPOS_NEW(m_pmp) CHashMapElem(pk, pt, true /*fOwn*/);
                (*ppdrgchain)->Append(phme);

                m_ulEntries++;

                m_pdrgKeys->Append(pk);

                return true;
            }
			
			// lookup a value by its key
			T *PtLookup(const K *pk) const
            {
                CHashMapElem *phme = NULL;
                Lookup(pk, &phme);
                if (NULL != phme)
                {
                    return phme->Pt();
                }

                return NULL;
            }

			// replace the value in a map entry with a new given value
			BOOL FReplace(const K *pk, T *ptNew)
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

			// return number of map entries
			ULONG UlEntries() const
			{
				return m_ulEntries;
			}		

	}; // class CHashMap

}

#endif // !GPOS_CHashMap_H

// EOF

