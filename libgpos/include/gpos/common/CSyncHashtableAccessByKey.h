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
			T *PtNextMatch(T *pt) const
            {
                T *ptCurrent = pt;

                while (NULL != ptCurrent &&
                       !Base::Sht().m_pfuncEqual(Base::Sht().Key(ptCurrent), m_key))
                {
                    ptCurrent = Base::PtNext(ptCurrent);
                }

                return ptCurrent;
            }

#ifdef GPOS_DEBUG
			// returns true if current bucket matches key
			BOOL FMatchingBucket(const K &key) const
            {
                ULONG ulBucketIndex = Base::Sht().UlBucketIndex(key);

                return &(Base::Sht().Bucket(ulBucketIndex)) == &(Base::Bucket());
            }
#endif // GPOS_DEBUG

		public:
	
			// ctor - acquires spinlock on target bucket
			CSyncHashtableAccessByKey<T, K, S>
				(CSyncHashtable<T, K, S> &ht, const K &key)
            :
            Base(ht, ht.UlBucketIndex(key)),
            m_key(key)
            {
            }
				
			// dtor
			virtual 
			~CSyncHashtableAccessByKey()
			{}

			// finds the first bucket's element with a matching key
			T *PtLookup() const
            {
                return PtNextMatch(Base::PtFirst());
            }

			// finds the next element with a matching key
			T *PtNext(T *pt) const
            {
                GPOS_ASSERT(NULL != pt);

                return PtNextMatch(Base::PtNext(pt));
            }

			// insert at head of target bucket's hash chain
			void Insert(T *pt)
            {
                GPOS_ASSERT(NULL != pt);

    #ifdef GPOS_DEBUG
                K &key = Base::Sht().Key(pt);
    #endif // GPOS_DEBUG

                // make sure this is a valid key
                GPOS_ASSERT(Base::Sht().FValid(key));

                // make sure this is the right bucket
                GPOS_ASSERT(FMatchingBucket(key));

                // inserting at bucket's head is required by hashtable iteration
                Base::Prepend(pt);
            }
		
	}; // class CSyncHashtableAccessByKey

}

#endif // !GPOS_CSyncHashtableAccessByKey_H

// EOF

