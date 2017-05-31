//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncHashtableAccessorBase.h
//
//	@doc:
//		Base hashtable accessor class; provides primitives to operate
//		on a target bucket in the hashtable.
//
//		Throughout its life time, the accessor holds the spinlock on a
//		target bucket; this allows clients to implement more complex
//		functionality than simple test-and-insert/remove functions.
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableAccessorBase_H_
#define GPOS_CSyncHashtableAccessorBase_H_

#include "gpos/base.h"

#include "gpos/common/CSyncHashtable.h"
#include "gpos/common/CSyncHashtableIter.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtableAccessorBase<T, K, S>
	//
	//	@doc:
	//		Accessor class to encapsulate locking of a hashtable bucket;
	//		has to know all template parameters of the hashtable class in order
	//		to link to the target hashtable; see file doc for more details on the
	//		rationale behind this class
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	class CSyncHashtableAccessorBase : public CStackObject
	{

		private:

			// shorthand for buckets
			typedef struct CSyncHashtable<T, K, S>::SBucket SBucket;

			// target hashtable
			CSyncHashtable<T, K, S> &m_ht;

			// bucket to operate on
			SBucket &m_bucket;

			// no copy ctor
			CSyncHashtableAccessorBase<T, K, S>
				(const CSyncHashtableAccessorBase<T, K, S>&);

		protected:

			// ctor - protected to restrict instantiation to children
			CSyncHashtableAccessorBase<T, K, S>
				(
				CSyncHashtable<T, K, S> &ht,
				ULONG ulBucketIndex
				)
            :
            m_ht(ht),
            m_bucket(m_ht.Bucket(ulBucketIndex))
            {
                // acquire spin lock on bucket
                m_bucket.m_slock.Lock();
            }

			// dtor
			virtual
			~CSyncHashtableAccessorBase<T, K, S>()
            {
                // unlock bucket
                m_bucket.m_slock.Unlock();
            }

			// accessor to hashtable
			CSyncHashtable<T, K, S>& Sht() const
			{
				return m_ht;
			}

			// accessor to maintained bucket
			SBucket& Bucket() const
			{
				return m_bucket;
			}

			// returns the first element in the hash chain
			T *PtFirst() const
            {
                return m_bucket.m_list.PtFirst();
            }

			// finds the element next to the given one
			T *PtNext(T *pt) const
            {
                GPOS_ASSERT(NULL != pt);

                // make sure element is in this hash chain
                GPOS_ASSERT(GPOS_OK == m_bucket.m_list.EresFind(pt));

                return m_bucket.m_list.PtNext(pt);
            }

			// inserts element at the head of hash chain
			void Prepend(T *pt)
            {
                GPOS_ASSERT(NULL != pt);

                m_bucket.m_list.Prepend(pt);

                // increase number of entries
                (void) UlpExchangeAdd(&(m_ht.m_ulpEntries), 1);
            }

			// adds first element before second element
			void Prepend(T *pt, T *ptNext)
            {
                GPOS_ASSERT(NULL != pt);

                // make sure element is in this hash chain
                GPOS_ASSERT(GPOS_OK == m_bucket.m_list.EresFind(ptNext));

                m_bucket.m_list.Prepend(pt, ptNext);

                // increase number of entries
                (void) UlpExchangeAdd(&(m_ht.m_ulpEntries), 1);
            }

			// adds first element after second element
			void Append(T *pt, T *ptPrev)
            {
                GPOS_ASSERT(NULL != pt);

                // make sure element is in this hash chain
                GPOS_ASSERT(GPOS_OK == m_bucket.m_list.EresFind(ptPrev));

                m_bucket.m_list.Append(pt, ptPrev);

                // increase number of entries
                (void) UlpExchangeAdd(&(m_ht.m_ulpEntries), 1);
            }

		public:

			// unlinks element
			void Remove(T *pt)
            {
                // not NULL and is-list-member checks are done in CList
                m_bucket.m_list.Remove(pt);

                // decrease number of entries
                (void) UlpExchangeAdd(&(m_ht.m_ulpEntries), -1);
            }

	}; // class CSyncHashtableAccessorBase

}

#endif // GPOS_CSyncHashtableAccessorBase_H_

// EOF
