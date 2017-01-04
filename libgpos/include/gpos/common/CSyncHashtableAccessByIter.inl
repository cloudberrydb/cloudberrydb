//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncHashtableAccessByIter.inl
//
//	@doc:
//		Implementation of hash table iterator's accessor
//---------------------------------------------------------------------------

namespace gpos
{
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByIter::CSyncHashtableAccessByIter
	//
	//	@doc:
	//		Ctor;
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	CSyncHashtableAccessByIter<T, K, S>::CSyncHashtableAccessByIter
		(
		CSyncHashtableIter<T, K, S> &iter
		)
		:
		Base(iter.m_ht, iter.m_ulBucketIndex),
		m_iter(iter)			
	{
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByIter::PtFirstValid
	//
	//	@doc:
	//		Finds the first valid element starting from the given element
	//		 
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessByIter<T, K, S>::PtFirstValid
		(
		T *pt		
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		
		T *ptCurrent = pt;
		while (NULL != ptCurrent && 
			   !Base::Sht().FValid(Base::Sht().Key(ptCurrent)))
		{
			ptCurrent = Base::PtNext(ptCurrent);
		}
		
		return ptCurrent;
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableAccessByIter::Pt
	//
	//	@doc:
	//		Returns the first valid element pointed to by iterator
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	T*
	CSyncHashtableAccessByIter<T, K, S>::Pt() const
	{ 
		GPOS_ASSERT(m_iter.m_fInvalidInserted && 
				    "Iterator's advance is not called");
		
		// advance in the current bucket until finding a valid element;
		// this is needed because the next valid element pointed to by
		// iterator might have been deleted by another client just before 
		// using the accessor
		
		return PtFirstValid(m_iter.m_ptInvalid);	
	}
	
	
} // namespace gpos


// EOF
