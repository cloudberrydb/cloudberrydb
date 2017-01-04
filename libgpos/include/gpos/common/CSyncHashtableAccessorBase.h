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
				);

			// dtor
			virtual
			~CSyncHashtableAccessorBase<T, K, S>();

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
			T *PtFirst() const;

			// finds the element next to the given one
			T *PtNext(T *pt) const;

			// inserts element at the head of hash chain
			void Prepend(T *pt);

			// adds first element before second element
			void Prepend(T *pt, T *ptNext);

			// adds first element after second element
			void Append(T *pt, T *ptPrev);

		public:

			// unlinks element
			void Remove(T *pt);

	}; // class CSyncHashtableAccessorBase

}

// include inline definition of accessor implementation
#include "gpos/common/CSyncHashtableAccessorBase.inl"

#endif // GPOS_CSyncHashtableAccessorBase_H_

// EOF
