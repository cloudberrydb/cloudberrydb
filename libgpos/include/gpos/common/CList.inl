//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CList.inl
//
//	@doc:
//		Template-based list class; 
//		In order to be useful for system programming the class must be 
//		allocation-less, i.e. manage elements without additional allocation,
//		to work in exceptions or OOM situations
//---------------------------------------------------------------------------

#include "gpos/task/ITask.h"
#include "gpos/_api.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Link
	//
	//	@doc:
	//		Extracts link information from element;
	//		Since there can be many link structures embedded we cannot access it
	//		directly but go by offset;
	//
	//---------------------------------------------------------------------------
	template<class T>
	SLink &
	CList<T>::Link
		(
		const void *pv
		)
		const
	{
		GPOS_ASSERT(MAX_ALIGNED(pv));
		GPOS_ASSERT(MAX_ALIGNED(m_cOffset));

		GPOS_ASSERT(ULONG_MAX != m_cOffset && "Link offset not initialized.");
		SLink &link = *(SLink*)(((BYTE*)pv) + m_cOffset);
		return link;
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Prepend
	//
	//	@doc:
	//		Insert element at head of list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CList<T>::Prepend
		(
		T *pt
		)
	{
		GPOS_ASSERT(NULL != pt);

		// inserting first element?
		if (NULL == m_ptHead)
		{					
			GPOS_ASSERT(NULL == m_ptTail);
			GPOS_ASSERT(0 == m_ulSize);
			SLink &sl = Link(pt);

			sl.m_pvNext = NULL;
			sl.m_pvPrev = NULL;

			m_ptHead = pt;
			m_ptTail = pt;

			m_ulSize++;
		}
		else
		{
			Prepend(pt, m_ptHead);
		}

		GPOS_ASSERT(0 != m_ulSize);
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Prepend
	//
	//	@doc:
	//		Insert element before given element;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CList<T>::Prepend
		(
		T *pt,
		T *ptNext
		)
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != ptNext);
		T *ptPrev = static_cast<T*>(Link(ptNext).m_pvPrev);
		
		SLink &sl = Link(pt);
		sl.m_pvNext = ptNext;
		sl.m_pvPrev = ptPrev;

		Link(ptNext).m_pvPrev = pt;
		if (NULL != ptPrev)
		{
			// inserted not at head, ie valid prev element
			Link(ptPrev).m_pvNext = pt;
		}
		else
		{
			// prepended to head
			m_ptHead = pt;
		}

		m_ulSize++;
	}
			

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Append
	//
	//	@doc:
	//		Insert element at the end of list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CList<T>::Append
		(
		T *pt
		)
	{
		GPOS_ASSERT(NULL != pt);

		if (NULL == m_ptTail)
		{
			Prepend(pt);
		}
		else
		{
			Append(pt, m_ptTail);
		}

		GPOS_ASSERT(0 != m_ulSize);
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Append
	//
	//	@doc:
	//		Insert element after given element;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CList<T>::Append
		(
		T *pt,
		T *ptPrev
		)
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != ptPrev);
		T *ptNext = static_cast<T*>(Link(ptPrev).m_pvNext);
		
		SLink &sl = Link(pt);
		sl.m_pvPrev = ptPrev;
		sl.m_pvNext = ptNext;

		Link(ptPrev).m_pvNext = pt;
		if (NULL != ptNext)
		{
			// inserted not at tail, ie valid next element
			Link(ptNext).m_pvPrev = pt;
		}
		else
		{
			// appended to tail
			m_ptTail = pt;
		}

		m_ulSize++;
	}
			

	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Remove
	//
	//	@doc:
	//		Remove given element from list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CList<T>::Remove
		(
		T *pt
		)
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != m_ptHead);
		GPOS_ASSERT(NULL != m_ptTail);
		GPOS_ASSERT(0 != m_ulSize);
		
		// ensure this element is actually member of the list,
		// this assert is expensive as it traverses the list to make sure
		// that an entry is accessible by following entries' next pointers
		GPOS_ASSERT_IMP(fEnableExtendedAsserts, GPOS_OK == EresFind(pt));

		SLink &sl = Link(pt);

		if (sl.m_pvNext)
		{
			Link(sl.m_pvNext).m_pvPrev = sl.m_pvPrev;
		}
		else
		{
			// removing tail element
			GPOS_ASSERT(m_ptTail == pt);
			m_ptTail = static_cast<T*>(sl.m_pvPrev);
		}
		
		if (sl.m_pvPrev)
		{
			Link(sl.m_pvPrev).m_pvNext = sl.m_pvNext;			
		}
		else
		{
			// removing head element
			GPOS_ASSERT(m_ptHead == pt);
			m_ptHead = static_cast<T*>(sl.m_pvNext);
		}
		
		// unlink element
		sl.m_pvPrev = NULL;
		sl.m_pvNext = NULL;

		m_ulSize--;
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Remove
	//
	//	@doc:
	//		Remove and return first element;
	//
	//---------------------------------------------------------------------------
	template<class T>
	T *
	CList<T>::RemoveHead()
	{
		T *ptHead = m_ptHead;
		Remove(m_ptHead);
		return ptHead;
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Remove
	//
	//	@doc:
	//		Remove and return last element;
	//
	//---------------------------------------------------------------------------
	template<class T>
	T *
	CList<T>::RemoveTail()
	{
		T *ptTail = m_ptTail;
		Remove(m_ptTail);
		return ptTail;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::PtNext
	//
	//	@doc:
	//		Get next element;
	//
	//---------------------------------------------------------------------------
	template<class T>
	T *
	CList<T>::PtNext
		(
		const T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != m_ptHead);
		GPOS_ASSERT(NULL != m_ptTail);
		
		SLink &sl = Link(pt);
		return static_cast<T*>(sl.m_pvNext);
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::PtPrev
	//
	//	@doc:
	//		Returns previous element;
	//
	//---------------------------------------------------------------------------
	template<class T>
	T *
	CList<T>::PtPrev
		(
		const T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(NULL != m_ptHead);
		GPOS_ASSERT(NULL != m_ptTail);

		SLink &sl = Link(pt);
		return static_cast<T*>(sl.m_pvPrev);
	}
	

#ifdef GPOS_DEBUG

	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::EresFind
	//
	//	@doc:
	//		Lookup element in list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	GPOS_RESULT
	CList<T>::EresFind
		(
		T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);

		// iterate until found
		T *ptCrs = PtFirst();
		while(ptCrs)
		{
			if (pt == ptCrs)
			{
				return GPOS_OK;
			}
			ptCrs = PtNext(ptCrs);
		}
		return GPOS_NOT_FOUND;
	}

	
	//---------------------------------------------------------------------------
	//	@function:
	//		CList<T>::Print
	//
	//	@doc:
	//		Simple debug print out of list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	gpos::IOstream &
	CList<T>::OsPrint
		(
		IOstream &os
		) 
		const
	{
		ULONG c = 0;
		
		T *ptCrs = PtFirst();
		
		do
		{
			os	<< "[" << c++ << "]"
				<< (void*)ptCrs 
				<< (NULL == ptCrs ? "" : " -> ");
		}
		while(ptCrs && (ptCrs = PtNext(ptCrs)));
		os << std::endl;
		
		return os;
	}
#endif // GPOS_DEBUG
}


// EOF
