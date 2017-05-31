//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncHashtableIter.h
//
//	@doc:
//		Iterator for allocation-less static hashtable; this class encapsulates
//		the state of iteration process (the current bucket we iterate through
//		and iterator position); it also allows advancing iterator's position
//		in the hash table; accessing the element at current iterator's
//		position is provided by the iterator's accessor class.
//
//		Since hash table iteration/insertion/deletion can happen concurrently,
//		the semantics of iteration output are defined as follows:
//			- the iterator sees all elements that have not been removed; if
//			any of them are removed concurrently, we may or may not see them
//			- New elements may or may not be seen
//			- No element is returned twice
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableIter_H
#define GPOS_CSyncHashtableIter_H



#include "gpos/base.h"

#include "gpos/common/clibwrapper.h"
#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableAccessByIter.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtableIter<T, K, S>
	//
	//	@doc:
	//		Iterator class has to know all template parameters of
	//		the hashtable class in order to link to a target hashtable.
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	class CSyncHashtableIter
	{

		// iterator accessor class is a friend
		friend class CSyncHashtableAccessByIter<T, K, S>;

		private:

			// target hashtable
			CSyncHashtable<T, K, S> &m_ht;

			// index of bucket to operate on
			ULONG m_ulBucketIndex;

			// a slab of memory to manufacture an invalid element; we enforce
			// memory alignment here to mimic allocation of a class object
			ALIGN_STORAGE BYTE m_rgInvalid[sizeof(T)];

			// a pointer to memory slab to interpret it as invalid element
			T *m_ptInvalid;

			// no copy ctor
			CSyncHashtableIter<T, K, S>(const CSyncHashtableIter<T, K, S>&);

			// inserts invalid element at the head of current bucket
			void InsertInvalidElement()
            {
                m_fInvalidInserted = false;
                while (m_ulBucketIndex < m_ht.m_cSize)
                {
                    CSyncHashtableAccessByIter<T, K, S> shtitacc(*this);

                    T *ptFirst = shtitacc.PtFirst();
                    T *ptFirstValid = NULL;

                    if (NULL != ptFirst &&
                        (NULL != (ptFirstValid = shtitacc.PtFirstValid(ptFirst))))
                    {
                        // insert invalid element before the found element
                        shtitacc.Prepend(m_ptInvalid, ptFirstValid);
                        m_fInvalidInserted = true;
                        break;
                    }
                    else
                    {
                        m_ulBucketIndex++;
                    }
                }
            }

			// advances invalid element in current bucket
			void AdvanceInvalidElement()
            {
                CSyncHashtableAccessByIter<T, K, S> shtitacc(*this);

                T *pt = shtitacc.PtFirstValid(m_ptInvalid);

                shtitacc.Remove(m_ptInvalid);
                m_fInvalidInserted = false;

                // check that we did not find the last element in bucket
                if (NULL != pt && NULL != shtitacc.PtNext(pt))
                {
                    // insert invalid element after the found element
                    shtitacc.Append(m_ptInvalid, pt);
                    m_fInvalidInserted = true;
                }
            }

			// a flag indicating if invalid element is currently in the hash table
			BOOL m_fInvalidInserted;

		public:

			// ctor
			explicit
			CSyncHashtableIter<T, K, S>(CSyncHashtable<T, K, S> &ht)
            :
            m_ht(ht),
            m_ulBucketIndex(0),
            m_ptInvalid(NULL),
            m_fInvalidInserted(false)
            {
                m_ptInvalid = (T*)m_rgInvalid;

                // get a reference to invalid element's key
                K &key = m_ht.Key(m_ptInvalid);

                // copy invalid key to invalid element's memory
                (void) clib::PvMemCpy
                (
                 &key,
                 m_ht.m_pkeyInvalid,
                 sizeof(*(m_ht.m_pkeyInvalid))
                 );
            }

			// dtor
			~CSyncHashtableIter<T, K, S>()
            {
                if (m_fInvalidInserted)
                {
                    // remove invalid element
                    CSyncHashtableAccessByIter<T, K, S> shtitacc(*this);
                    shtitacc.Remove(m_ptInvalid);
                }
            }

			// advances iterator
			BOOL FAdvance()
            {
                GPOS_ASSERT(m_ulBucketIndex < m_ht.m_cSize &&
                            "Advancing an exhausted iterator");

                if (!m_fInvalidInserted)
                {
                    // first time to call iterator's advance, insert invalid
                    // element into the first non-empty bucket
                    InsertInvalidElement();
                }
                else
                {
                    AdvanceInvalidElement();

                    if (!m_fInvalidInserted)
                    {
                        // current bucket is exhausted, insert invalid element
                        // in the next unvisited non-empty bucket
                        m_ulBucketIndex++;
                        InsertInvalidElement();
                    }
                }

                return (m_ulBucketIndex < m_ht.m_cSize);
            }

			// rewinds the iterator to the beginning
			void RewindIterator()
            {
                GPOS_ASSERT(m_ulBucketIndex >= m_ht.m_cSize &&
                            "Rewinding an un-exhausted iterator");
                GPOS_ASSERT(!m_fInvalidInserted && "Invalid element from previous iteration exists, cannot rewind");

                m_ulBucketIndex = 0;
            }

	}; // class CSyncHashtableIter

}

#endif // !GPOS_CSyncHashtableIter_H

// EOF

