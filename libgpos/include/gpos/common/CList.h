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
			SLink &Link(const void *pv) const; 
		
		public:
				
			// ctor
			CList() 
				:
				m_cOffset(ULONG_MAX),
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
			void Prepend(T *pt);
			void Append(T *pt);

			// Insert by prepending/appending relative to given position
			void Prepend(T *pt, T *ptNext);
			void Append(T *pt, T *ptPrev);
						
			// remove element by navigating and manipulating its SLink member
			void Remove(T *pt);
			
			// remove and return first element
			T *RemoveHead();

			// remove and return last element
			T *RemoveTail();

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
			T *PtNext(const T *pt) const;
			
			// and backward...
			T *PtPrev(const T *pt) const;
			
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
			GPOS_RESULT EresFind(T *pt) const ;

			// debug print of element addresses
			IOstream &OsPrint(IOstream &) const;
#endif // GPOS_DEBUG

	}; // class CList<T>
}


// include implementation
#include "CList.inl"

#endif // !GPOS_CList_H

// EOF

