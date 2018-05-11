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
#include "gpos/sync/atomic.h"

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
			void CheckBackOff(ULONG &attempts) const
            {
                if (++attempts == GPOS_SPIN_ATTEMPTS)
                {
                    // back-off
                    clib::USleep(GPOS_SPIN_BACKOFF);

                    attempts = 0;

                    GPOS_CHECK_ABORT;
                }
            }

			// no copy ctor
			CSyncList(const CSyncList&);

		public:

			// ctor
			CSyncList()
			{}

			// dtor
			~CSyncList()
            {}

			// init function to facilitate arrays
			void Init
				(
				ULONG offset
				)
			{
				m_list.Init(offset);
			}

			// insert element at the head of the list;
			void Push(T *elem)
            {
                GPOS_ASSERT(NULL != elem);
                GPOS_ASSERT(m_list.First() != elem);

                ULONG attempts = 0;
                SLink &link = m_list.Link(elem);

    #ifdef GPOS_DEBUG
                void *next_head = link.m_next;
    #endif // GPOS_DEBUG

                GPOS_ASSERT(NULL == link.m_next);

                // keep spinning until passed element becomes the head
                while (true)
                {
                    T *head = m_list.First();

                    GPOS_ASSERT(elem != head && "Element is already inserted");
                    GPOS_ASSERT(next_head == link.m_next && "Element is concurrently accessed");

                    // set current head as next element
                    link.m_next = head;
    #ifdef GPOS_DEBUG
                    next_head = link.m_next;
    #endif // GPOS_DEBUG

                    // attempt to set element as head
                    if (CompareSwap<T>((volatile T**) &m_list.m_head, head, elem))
                    {
                        break;
                    }

                    CheckBackOff(attempts);
                }
            }

			// remove element from the head of the list;
			T *Pop()
            {
                ULONG attempts = 0;
                T *old_head = NULL;

                // keep spinning until the head is removed
                while (true)
                {
                    // get current head
                    old_head = m_list.First();
                    if (NULL == old_head)
                    {
                        break;
                    }

                    // second element becomes the new head
                    SLink &link = m_list.Link(old_head);
                    T *new_head = static_cast<T*>(link.m_next);

                    // attempt to set new head
                    if (CompareSwap<T>((volatile T**) &m_list.m_head, old_head, new_head))
                    {
                        // reset link
                        link.m_next = NULL;
                        break;
                    }

                    CheckBackOff(attempts);
                }

                return old_head;
            }

			// get first element
			T *PtFirst()
			{
				m_list.m_tail = m_list.m_head;
				return m_list.First();
			}

			// get next element
			T *Next
				(
				T *elem
				)
			{
				m_list.m_tail = m_list.m_head;
				return m_list.Next(elem);
			}

			// check if list is empty
			BOOL IsEmpty() const
			{
				return NULL == m_list.First();
			}

#ifdef GPOS_DEBUG

			// lookup a given element in the stack
			// this works only when no elements are removed
			GPOS_RESULT
			Find
				(
				T *elem
				)
			{
				m_list.m_tail = m_list.m_head;
				return m_list.Find(elem);
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

#endif // !GPOS_CSyncList_H


// EOF
