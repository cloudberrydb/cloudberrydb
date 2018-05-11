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
				ULONG bucket_idx
				)
            :
            m_ht(ht),
            m_bucket(m_ht.GetBucket(bucket_idx))
            {
                // acquire spin lock on bucket
                m_bucket.m_lock.Lock();
            }

			// dtor
			virtual
			~CSyncHashtableAccessorBase<T, K, S>()
            {
                // unlock bucket
                m_bucket.m_lock.Unlock();
            }

			// accessor to hashtable
			CSyncHashtable<T, K, S>& GetHashTable() const
			{
				return m_ht;
			}

			// accessor to maintained bucket
			SBucket& GetBucket() const
			{
				return m_bucket;
			}

			// returns the first element in the hash chain
			T *First() const
            {
                return m_bucket.m_chain.First();
            }

			// finds the element next to the given one
			T *Next(T *value) const
            {
                GPOS_ASSERT(NULL != value);

                // make sure element is in this hash chain
                GPOS_ASSERT(GPOS_OK == m_bucket.m_chain.Find(value));

                return m_bucket.m_chain.Next(value);
            }

			// inserts element at the head of hash chain
			void Prepend(T *value)
            {
                GPOS_ASSERT(NULL != value);

                m_bucket.m_chain.Prepend(value);

                // increase number of entries
                (void) ExchangeAddUlongPtrWithInt(&(m_ht.m_size), 1);
            }

			// adds first element before second element
			void Prepend(T *value, T *ptNext)
            {
                GPOS_ASSERT(NULL != value);

                // make sure element is in this hash chain
                GPOS_ASSERT(GPOS_OK == m_bucket.m_chain.Find(ptNext));

                m_bucket.m_chain.Prepend(value, ptNext);

                // increase number of entries
                (void) ExchangeAddUlongPtrWithInt(&(m_ht.m_size), 1);
            }

			// adds first element after second element
			void Append(T *value, T *ptPrev)
            {
                GPOS_ASSERT(NULL != value);

                // make sure element is in this hash chain
                GPOS_ASSERT(GPOS_OK == m_bucket.m_chain.Find(ptPrev));

                m_bucket.m_chain.Append(value, ptPrev);

                // increase number of entries
                (void) ExchangeAddUlongPtrWithInt(&(m_ht.m_size), 1);
            }

		public:

			// unlinks element
			void Remove(T *value)
            {
                // not NULL and is-list-member checks are done in CList
                m_bucket.m_chain.Remove(value);

                // decrease number of entries
                (void) ExchangeAddUlongPtrWithInt(&(m_ht.m_size), -1);
            }

	}; // class CSyncHashtableAccessorBase

}

#endif // GPOS_CSyncHashtableAccessorBase_H_

// EOF
