//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtableAccessByKey.h
//
//	@doc:
//		Accessor for allocation-less static hashtable;
//		The Accessor is instantiated with a target key. Throughout its life
//		time, the accessor holds the spinlock on the target key's bucket --
//		regardless of whether or not the key exists in the hashtable; this 
//		allows clients to implement more complex functionality than simple
//		test-and-insert/remove functions; acquiring and releasing locks is
//		done by the parent CSyncHashtableAccessorBase class.
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncHashtableAccessByKey_H
#define GPOS_CSyncHashtableAccessByKey_H


#include "gpos/common/CSyncHashtableAccessorBase.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncHashtableAccessByKey<T, K, S>
	//
	//	@doc:
	//		Accessor class to encapsulate locking of a hashtable bucket based on
	//		a passed key; has to know all template parameters of the hashtable
	//		class in order to link to the target hashtable; see file doc for more
	//		details on the rationale behind this class
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	class CSyncHashtableAccessByKey : public CSyncHashtableAccessorBase<T, K, S>
	{

		private:

			// shorthand for accessor's base class
			typedef class CSyncHashtableAccessorBase<T, K, S> Base;

			// target key
			const K &m_key;

			// no copy ctor
			CSyncHashtableAccessByKey<T, K, S>
				(const CSyncHashtableAccessByKey<T, K, S>&);
		
			// finds the first element matching target key starting from
			// the given element
			T *PtNextMatch(T *pt) const;

#ifdef GPOS_DEBUG
			// returns true if current bucket matches key
			BOOL FMatchingBucket(const K &key) const;
#endif // GPOS_DEBUG

		public:
	
			// ctor - acquires spinlock on target bucket
			CSyncHashtableAccessByKey<T, K, S>
				(CSyncHashtable<T, K, S> &ht, const K &key);
				
			// dtor
			virtual 
			~CSyncHashtableAccessByKey()
			{}

			// finds the first bucket's element with a matching key
			T *PtLookup() const;
		
			// finds the next element with a matching key
			T *PtNext(T *pt) const;

			// insert at head of target bucket's hash chain
			void Insert(T *pt);
		
	}; // class CSyncHashtableAccessByKey

}

// include implementation
#include "gpos/common/CSyncHashtableAccessByKey.inl"

#endif // !GPOS_CSyncHashtableAccessByKey_H

// EOF

