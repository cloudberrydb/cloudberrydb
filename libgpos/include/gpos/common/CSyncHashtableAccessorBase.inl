//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CSyncHashtableAccessorBase.inl
//
//	@doc:
//		Implementation of hash table base accessor
//---------------------------------------------------------------------------

namespace gpos
{

		
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::CSyncHashtableAccessorBase
	//
	//	@doc:
	//		Ctor;
	//		acquires spinlock on the given bucket
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	CSyncHashtableAccessorBase<T, K, S>::CSyncHashtableAccessorBase
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
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::~CSyncHashtableAccessorBase
	//
	//	@doc:
	//		Dtor;
	//		releases spinlock on the maintained bucket.
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	CSyncHashtableAccessorBase<T, K, S>::~CSyncHashtableAccessorBase()
	{	
		// unlock bucket
		m_bucket.m_slock.Unlock();
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::PtFirst
	//
	//	@doc:
	//		Finds the first element in the maintained bucket
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessorBase<T, K, S>::PtFirst() const
	{
		return m_bucket.m_list.PtFirst();
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::PtNext
	//
	//	@doc:
	//		Finds the element in the maintained bucket next to given element
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessorBase<T, K, S>::PtNext
		(
		T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		
		// make sure element is in this hash chain
		GPOS_ASSERT(GPOS_OK == m_bucket.m_list.EresFind(pt));
		
		return m_bucket.m_list.PtNext(pt);
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::Prepend
	//
	//	@doc:
	//		Adds element by prepending to the maintained bucket
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableAccessorBase<T, K, S>::Prepend
		(
		T *pt
		)
	{
		GPOS_ASSERT(NULL != pt);
		
		m_bucket.m_list.Prepend(pt);
		
		// increase number of entries
		(void) UlpExchangeAdd(&(m_ht.m_ulpEntries), 1);
	}
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::Prepend
	//
	//	@doc:
	//		Adds element before another element
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableAccessorBase<T, K, S>::Prepend
		(
		T *pt,
		T *ptNext
		)
	{
		GPOS_ASSERT(NULL != pt);
		
		// make sure element is in this hash chain
		GPOS_ASSERT(GPOS_OK == m_bucket.m_list.EresFind(ptNext));
		
		m_bucket.m_list.Prepend(pt, ptNext);
		
		// increase number of entries
		(void) UlpExchangeAdd(&(m_ht.m_ulpEntries), 1);
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::Append
	//
	//	@doc:
	//		Adds element next to another element
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableAccessorBase<T, K, S>::Append
		(
		T *pt,
		T *ptPrev
		)
	{	
		GPOS_ASSERT(NULL != pt);
		
		// make sure element is in this hash chain
		GPOS_ASSERT(GPOS_OK == m_bucket.m_list.EresFind(ptPrev));
		
		m_bucket.m_list.Append(pt, ptPrev);
		
		// increase number of entries
		(void) UlpExchangeAdd(&(m_ht.m_ulpEntries), 1);
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessorBase::Remove
	//
	//	@doc:
	//		Removes given element from the the maintained bucket
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableAccessorBase<T, K, S>::Remove
		(
		T *pt
		)
	{
		// not NULL and is-list-member checks are done in CList
		m_bucket.m_list.Remove(pt);
		
		// decrease number of entries
		(void) UlpExchangeAdd(&(m_ht.m_ulpEntries), -1);
	}
	
	
} // namespace gpos


// EOF
