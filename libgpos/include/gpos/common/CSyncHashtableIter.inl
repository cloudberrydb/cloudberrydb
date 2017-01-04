//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncHashtableIterator.inl
//
//	@doc:
//		Implementation of hash table iterator
//---------------------------------------------------------------------------

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableIter::CSyncHashtableIter
	//
	//	@doc:
	//		Ctor; manufactures invalid element out of memory slab
	//
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	CSyncHashtableIter<T, K, S>::CSyncHashtableIter
		(
		CSyncHashtable<T, K, S> &ht
		)
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

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableIter::~CSyncHashtableIter
	//
	//	@doc:
	//		Dtor; unlinks invalid element from the hash table
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	CSyncHashtableIter<T, K, S>::~CSyncHashtableIter()
	{
		if (m_fInvalidInserted)
		{
			// remove invalid element
			CSyncHashtableAccessByIter<T, K, S> shtitacc(*this);
			shtitacc.Remove(m_ptInvalid);
		}			
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableIter::InsertInvalidElement
	//
	//	@doc:
	//		Inserts invalid element into the first unvisited bucket with at
	//		least one valid element
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableIter<T, K, S>::InsertInvalidElement()
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
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableIter::AdvanceInvalidElement
	//
	//	@doc:
	//		Advances invalid element within the current bucket; if current 
	//		bucket is not yet exhausted, the invalid element's position is 
	//		advanced, otherwise the invalid element is removed from current
	//		bucket
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	void
	CSyncHashtableIter<T, K, S>::AdvanceInvalidElement()
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
	
		
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableIter::FAdvance
	//
	//	@doc:
	//		Advances iterator position; returns true if iteration is not 
	//		completed yet
	//
	//---------------------------------------------------------------------------	
	template <class T, class K, class S>
	BOOL
	CSyncHashtableIter<T, K, S>::FAdvance()
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
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncHashtableIter::RewindIterator
	//
	//	@doc:
	//		Rewinds the iterator to the beginning; i.e., starts from the first hash bucket again
	//
	//---------------------------------------------------------------------------
	template <class T, class K, class S>
	void
	CSyncHashtableIter<T, K, S>::RewindIterator()
	{
		GPOS_ASSERT(m_ulBucketIndex >= m_ht.m_cSize &&
				    "Rewinding an un-exhausted iterator");
		GPOS_ASSERT(!m_fInvalidInserted && "Invalid element from previous iteration exists, cannot rewind");

		m_ulBucketIndex = 0;
	}
} // namespace gpos


// EOF
