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
			void InsertInvalidElement();

			// advances invalid element in current bucket
			void AdvanceInvalidElement();

			// a flag indicating if invalid element is currently in the hash table
			BOOL m_fInvalidInserted;

		public:

			// ctor
			explicit
			CSyncHashtableIter<T, K, S>(CSyncHashtable<T, K, S> &ht);

			// dtor
			~CSyncHashtableIter<T, K, S>();

			// advances iterator
			BOOL FAdvance();

			// rewinds the iterator to the beginning
			void RewindIterator();

	}; // class CSyncHashtableIter

}

// include implementation
#include "gpos/common/CSyncHashtableIter.inl"

#endif // !GPOS_CSyncHashtableIter_H

// EOF

