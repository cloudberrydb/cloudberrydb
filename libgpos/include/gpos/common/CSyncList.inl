//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncList.inl
//
//	@doc:
//		Implementation of template-based synchronized stack class with
//		minimum synchronization overhead; it provides a minimal set of
//		thread-safe operations through atomic primitives (lock-free);
//
//		It requires that the elements are only inserted once, as their address
//		is used for identification; if elements are concurrently deleted,
//		iteration through the list is not thread safe;
//
//		In order to be useful for system programming the class must be
//		allocation-less, i.e. manage elements without additional allocation,
//		to work in exception or OOM situations;
//---------------------------------------------------------------------------

#include "gpos/sync/atomic.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncList<T>::~CSyncList
	//
	//	@doc:
	//		Dtor;
	//
	//---------------------------------------------------------------------------
	template<class T>
	CSyncList<T>::~CSyncList()
	{}


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncList<T>::Push
	//
	//	@doc:
	//		Insert element at the head of the list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CSyncList<T>::Push
		(
		T *pt
		)
	{
		GPOS_ASSERT(NULL != pt);
		GPOS_ASSERT(m_list.PtFirst() != pt);

		ULONG ulAttempts = 0;
		SLink &linkElem = m_list.Link(pt);

#ifdef GPOS_DEBUG
		void *pvHeadNext = linkElem.m_pvNext;
#endif // GPOS_DEBUG

		GPOS_ASSERT(NULL == linkElem.m_pvNext);

		// keep spinning until passed element becomes the head
		while (true)
		{
			T *ptHead = m_list.PtFirst();

			GPOS_ASSERT(pt != ptHead && "Element is already inserted");
			GPOS_ASSERT(pvHeadNext == linkElem.m_pvNext && "Element is concurrently accessed");

			// set current head as next element
			linkElem.m_pvNext = ptHead;
#ifdef GPOS_DEBUG
			pvHeadNext = linkElem.m_pvNext;
#endif // GPOS_DEBUG

			// attempt to set element as head
			if (FCompareSwap<T>((volatile T**) &m_list.m_ptHead, ptHead, pt))
			{
				break;
			}

			CheckBackOff(ulAttempts);
		}
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncList<T>::Pop
	//
	//	@doc:
	//		Remove element from the head of the list;
	//
	//---------------------------------------------------------------------------
	template<class T>
	T *
	CSyncList<T>::Pop()
	{
		ULONG ulAttempts = 0;
		T *ptHeadOld = NULL;

		// keep spinning until the head is removed
		while (true)
		{
			// get current head
			ptHeadOld = m_list.PtFirst();
			if (NULL == ptHeadOld)
			{
				break;
			}

			// second element becomes the new head
			SLink &linkElem = m_list.Link(ptHeadOld);
			T *ptHeadNew = static_cast<T*>(linkElem.m_pvNext);

			// attempt to set new head
			if (FCompareSwap<T>((volatile T**) &m_list.m_ptHead, ptHeadOld, ptHeadNew))
			{
				// reset link
				linkElem.m_pvNext = NULL;
				break;
			}

			CheckBackOff(ulAttempts);
		}

		return ptHeadOld;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CSyncList<T>::CheckBackOff
	//
	//	@doc:
	//		Back-off after too many attempts;
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CSyncList<T>::CheckBackOff
		(
		ULONG &ulAttempts
		)
		const
	{
		if (++ulAttempts == GPOS_SPIN_ATTEMPTS)
		{
			// back-off
			clib::USleep(GPOS_SPIN_BACKOFF);

			ulAttempts = 0;

			GPOS_CHECK_ABORT;
		}
	}
}



// EOF
