//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CHashSet.h
//
//	@doc:
//		Hash set
//		* stores deep objects, i.e., pointers
//		* equality == on objects uses template function argument
//		* does not allow insertion of duplicates
//		* destroys objects based on client-side provided destroy functions
//
//	@owner:
//		solimm1
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CHashSet_H
#define GPOS_CHashSet_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{

	// fwd declaration
	template <class T,
		ULONG (*pfnHash)(const T*),
		BOOL (*pfnEq)(const T*, const T*),
		void (*pfnDestroy)(T*)>
	class CHashSetIter;

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet
	//
	//	@doc:
	//		Hash set
	//
	//---------------------------------------------------------------------------
	template <class T,
				ULONG (*pfnHash)(const T*),
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	class CHashSet : public CRefCount
	{
		// fwd declaration
		friend class CHashSetIter<T, pfnHash, pfnEq, pfnDestroy>;

		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		CHashSetElem
			//
			//	@doc:
			//		Anchor for set element
			//
			//---------------------------------------------------------------------------
			class CHashSetElem
			{
				private:

					// pointer to object
					T *m_pt;

					// does hash set element own object?
					BOOL m_fOwn;

					// private copy ctor
					CHashSetElem(const CHashSetElem &);

				public:

					// ctor
					CHashSetElem(T *pt, BOOL fOwn)
                    :
                    m_pt(pt),
                    m_fOwn(fOwn)
                    {
                        GPOS_ASSERT(NULL != pt);
                    }

					// dtor
					~CHashSetElem()
                    {
                        // in case of a temporary HashSet element for lookup we do NOT own the
                        // objects, otherwise call destroy functions
                        if (m_fOwn)
                        {
                            pfnDestroy(m_pt);
                        }
                    }

					// object accessor
					T *Pt() const
					{
						return m_pt;
					}

					// equality operator
					BOOL operator == (const CHashSetElem &hse) const
					{
						return pfnEq(m_pt, hse.m_pt);
					}
			};	// class CHashSetElem

			// memory pool
			IMemoryPool *m_pmp;

			// number of hash chains
			ULONG m_ulSize;

			// total number of entries
			ULONG m_ulEntries;

			// each hash chain is an array of hashset elements
			typedef CDynamicPtrArray<CHashSetElem, CleanupDelete> DrgHashChain;
			DrgHashChain **m_ppdrgchain;

			// array for elements
			// We use CleanupNULL because the elements are owned by the hash table
			typedef CDynamicPtrArray<T, CleanupNULL> DrgElements;
			DrgElements *const m_pdrgElements;

			DrgPi *const m_pdrgPiFilledBuckets;

			// private copy ctor
			CHashSet(const CHashSet<T, pfnHash, pfnEq, pfnDestroy> &);

			// lookup appropriate hash chain in static table, may be NULL if
			// no elements have been inserted yet
			DrgHashChain **PpdrgChain(const T *pt) const
			{
				GPOS_ASSERT(NULL != m_ppdrgchain);
				return &m_ppdrgchain[pfnHash(pt) % m_ulSize];
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
			void Lookup(const T *pt, CHashSetElem **pphse) const
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

		public:

			// ctor
			CHashSet<T, pfnHash, pfnEq, pfnDestroy> (IMemoryPool *pmp, ULONG ulSize = 128)
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
			~CHashSet<T, pfnHash, pfnEq, pfnDestroy> ()
            {
                // release all hash chains
                Clear();

                GPOS_DELETE_ARRAY(m_ppdrgchain);
                m_pdrgElements->Release();
                m_pdrgPiFilledBuckets->Release();
            }

			// insert an element if not present
			BOOL FInsert(T *pt)
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

			// lookup element
			BOOL FExists(const T *pt) const
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

			// return number of map entries
			ULONG UlEntries() const
			{
				return m_ulEntries;
			}

	}; // class CHashSet

}

#endif // !GPOS_CHashSet_H

// EOF

