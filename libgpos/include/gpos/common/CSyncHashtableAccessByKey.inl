//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtableAccessByKey.inl
//
//	@doc:
//		Implementation of hash table accessor
//---------------------------------------------------------------------------

namespace gpos
{
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByKey::CSyncHashtableAccessByKey
	//
	//	@doc:
	//		Ctor
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	CSyncHashtableAccessByKey<T, K, S>::CSyncHashtableAccessByKey
		(
		CSyncHashtable<T, K, S> &ht,
		const K &key
		)
		:
		Base(ht, ht.UlBucketIndex(key)),
		m_key(key)	
	{
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByKey::PtNextMatch
	//
	//	@doc:
	//		Find the first element matching target key starting from the given 
	//		element; since target key is valid, we guarantee returning only valid 
	//		elements 
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessByKey<T, K, S>::PtNextMatch
		(
		T *pt
		)
		const
	{	
		T *ptCurrent = pt;
		
		while (NULL != ptCurrent && 
			   !Base::Sht().m_pfuncEqual(Base::Sht().Key(ptCurrent), m_key))
		{
			ptCurrent = Base::PtNext(ptCurrent);
		}
		
		return ptCurrent;
	}	
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByKey::PtLookup
	//
	//	@doc:
	//		Find first element with matching key in hash chain
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessByKey<T, K, S>::PtLookup() const
	{
		return PtNextMatch(Base::PtFirst());
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByKey::PtNext
	//
	//	@doc:
	//		Find the next element in hash chain matching target key
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessByKey<T, K, S>::PtNext
		(
		T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		
		return PtNextMatch(Base::PtNext(pt));
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByKey::Insert
	//
	//	@doc:
	//		Insert element by prepending to current bucket's hash chain
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableAccessByKey<T, K, S>::Insert
		(
		T *pt
		)
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
	
	
#ifdef GPOS_DEBUG	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByKey::FMatchingBucket
	//
	//	@doc:
	//		Check if we maintain the right bucket for the given key  
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	BOOL
	CSyncHashtableAccessByKey<T, K, S>::FMatchingBucket
		(
		const K &key
		)
		const
	{
		ULONG ulBucketIndex = Base::Sht().UlBucketIndex(key);
		
		return &(Base::Sht().Bucket(ulBucketIndex)) == &(Base::Bucket());
	}
#endif // GPOS_DEBUG	
	
	
} // namespace gpos


// EOF
