
//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CList.h
//
//	@doc:
//		Template-based list class; 
//		In order to be useful for system programming the class must be 
//		allocation-less, i.e. manage elements without additional allocation,
//		to work in exception or OOM situations
//---------------------------------------------------------------------------
#ifndef GPOS_CList_H
#define GPOS_CList_H

#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/task/ITask.h"
#include "gpos/_api.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		SLink
	//
	//	@doc:
	//		Generic link to be embedded in all classes before they can use
	//		allocation-less lists, e.g. in synchronized hashtables etc.
	//
	//---------------------------------------------------------------------------
	struct SLink
	{

		private:
		
			// no copy constructor
			SLink(const SLink&);

		public:

			// link forward/backward
			void *m_pvNext;
			void *m_pvPrev;
			
			// ctor
			SLink()
				:
				m_pvNext(NULL),
				m_pvPrev(NULL)
			{}
	};

	// forward declaration
	template <class T> class CSyncList;

	//---------------------------------------------------------------------------
	//	@class:
	//		CList<class T>
	//
	//	@doc:
	//		Allocation-less list class; requires T to have SLink embedded and
	//		N being the offset of this member;
	//
	//		Unfortunately not all compilers support use of an offset macro as
	//		template parameter; hence, we have to provide offset as parameter to
	//		constructor
	//
	//---------------------------------------------------------------------------
	template<class T>
	class CList
	{
		// friends
		friend class CSyncList<T>;

		private:
		
			// offest of link element
			ULONG m_cOffset;
		
			// size
			ULONG m_ulSize;

			// head element
			T *m_ptHead;

			// tail element
			T *m_ptTail;
			
			// no copy ctor
			CList(const CList&);
		
			// extract link from element
			SLink &Link(const void *pv) const
            {
                GPOS_ASSERT(MAX_ALIGNED(pv));
                GPOS_ASSERT(MAX_ALIGNED(m_cOffset));

                GPOS_ASSERT(gpos::ulong_max != m_cOffset &&
                            "Link offset not initialized.");
                SLink &link = *(SLink*)(((BYTE*)pv) + m_cOffset);
                return link;
            }
		
		public:
				
			// ctor
			CList()
				:
				m_cOffset(gpos::ulong_max),
				m_ulSize(0),
				m_ptHead(NULL),
				m_ptTail(NULL)
			{}
			
			// init function to facilitate arrays
			void Init
				(
				ULONG cOffset
				)
			{
				GPOS_ASSERT(MAX_ALIGNED(cOffset));
				m_cOffset = cOffset;
			}
			
			// Insert by prepending/appending to current list
			void Prepend(T *pt)
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

			void Append(T *pt)
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

			// Insert by prepending/appending relative to given position
			void Prepend(T *pt, T *ptNext)
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

			void Append(T *pt, T *ptPrev)
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
						
			// remove element by navigating and manipulating its SLink member
			void Remove(T *pt)
            {
                GPOS_ASSERT(NULL != pt);
                GPOS_ASSERT(NULL != m_ptHead);
                GPOS_ASSERT(NULL != m_ptTail);
                GPOS_ASSERT(0 != m_ulSize);

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
			
			// remove and return first element
			T *RemoveHead()
            {
                T *ptHead = m_ptHead;
                Remove(m_ptHead);
                return ptHead;
            }

			// remove and return last element
			T *RemoveTail()
            {
                T *ptTail = m_ptTail;
                Remove(m_ptTail);
                return ptTail;
            }

			// accessor to head of list
			T *
			PtFirst() const
			{
				return m_ptHead;
			}
			
			// accessor to tail of list
			T *
			PtLast() const
			{
				return m_ptTail;
			}

			// traversal functions PtNext and PtPrev assume
			// that the pointer passed is a valid member of the list
			T *PtNext(const T *pt) const
            {
                GPOS_ASSERT(NULL != pt);
                GPOS_ASSERT(NULL != m_ptHead);
                GPOS_ASSERT(NULL != m_ptTail);

                SLink &sl = Link(pt);
                return static_cast<T*>(sl.m_pvNext);
            }
			
			// and backward...
			T *PtPrev(const T *pt) const
            {
                GPOS_ASSERT(NULL != pt);
                GPOS_ASSERT(NULL != m_ptHead);
                GPOS_ASSERT(NULL != m_ptTail);

                SLink &sl = Link(pt);
                return static_cast<T*>(sl.m_pvPrev);
            }
			
			// get size
			ULONG UlSize() const
			{
				return m_ulSize;
			}

			// check if empty
			BOOL 
			FEmpty() const
			{
				return NULL == PtFirst();
			}

#ifdef GPOS_DEBUG
			// lookup a given element in the list
			GPOS_RESULT EresFind(T *pt) const
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

			// debug print of element addresses
			IOstream &OsPrint(IOstream &os) const
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

	}; // class CList<T>
}

#endif // !GPOS_CList_H

// EOF

