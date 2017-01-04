//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSyncList.h
//
//	@doc:
//		Template-based synchronized list class with minimum synchronization
//		overhead; it provides a minimal set of thread-safe operations through
//		atomic primitives (lock-free);
//
//		It requires that the elements are only inserted once, as their address
//		is used for identification; if elements are concurrently deleted,
//		iteration through the list is not thread safe;
//
//		In order to be useful for system programming the class must be
//		allocation-less, i.e. manage elements without additional allocation,
//		to work in exception or OOM situations;
//---------------------------------------------------------------------------
#ifndef GPOS_CSyncList_H
#define GPOS_CSyncList_H

#include "gpos/types.h"

#include "gpos/common/CList.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CSyncList<class T>
	//
	//	@doc:
	//		Allocation-less list class; requires T to have a pointer to T embedded
	//		and N being the offset of this member;
	//
	//---------------------------------------------------------------------------
	template<class T>
	class CSyncList
	{
		private:

			// underlying list
			CList<T> m_list;

			// back-off after too many attempts
			void CheckBackOff(ULONG &ulAttempts) const;

			// no copy ctor
			CSyncList(const CSyncList&);

		public:

			// ctor
			CSyncList()
			{}

			// dtor
			~CSyncList();

			// init function to facilitate arrays
			void Init
				(
				ULONG ulOffset
				)
			{
				m_list.Init(ulOffset);
			}

			// insert element at the head of the list;
			void Push(T *pt);

			// remove element from the head of the list;
			T *Pop();

			// get first element
			T *PtFirst()
			{
				m_list.m_ptTail = m_list.m_ptHead;
				return m_list.PtFirst();
			}

			// get next element
			T *PtNext
				(
				T *pt
				)
			{
				m_list.m_ptTail = m_list.m_ptHead;
				return m_list.PtNext(pt);
			}

			// check if list is empty
			BOOL FEmpty() const
			{
				return NULL == m_list.PtFirst();
			}

#ifdef GPOS_DEBUG

			// lookup a given element in the stack
			// this works only when no elements are removed
			GPOS_RESULT
			EresFind
				(
				T *pt
				)
			{
				m_list.m_ptTail = m_list.m_ptHead;
				return m_list.EresFind(pt);
			}

			// debug print of element addresses
			// this works only when no elements are removed
			IOstream &
			OsPrint
				(
				IOstream &os
				)
				const
			{
				return m_list.OsPrint(os);
			}

#endif // GPOS_DEBUG

	}; // class CSyncList
}


// include implementation
#include "CSyncList.inl"

#endif // !GPOS_CSyncList_H


// EOF
