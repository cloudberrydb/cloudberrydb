//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CRefCount.h
//
//	@doc:
//		Base class for reference counting in the optimizer; 
//		Ref-counting is single-threaded only;
//		Enforces allocation on the heap, i.e. no deletion unless the 
//		count has really dropped to zero
//---------------------------------------------------------------------------
#ifndef GPOS_CRefCount_H
#define GPOS_CRefCount_H

#include "gpos/utils.h"
#include "gpos/assert.h"
#include "gpos/sync/atomic.h"
#include "gpos/types.h"

#include "gpos/error/CException.h"
#include "gpos/common/CHeapObject.h"
#include "gpos/task/ITask.h"

#ifdef GPOS_32BIT
#define GPOS_WIPED_MEM_PATTERN		0xCcCcCcCc
#else
#define GPOS_WIPED_MEM_PATTERN		0xCcCcCcCcCcCcCcCc
#endif

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CRefCount
	//
	//	@doc:
	//		Basic reference counting
	//
	//---------------------------------------------------------------------------
	class CRefCount : public CHeapObject
	{
		private:
		
			// reference counter -- first in class to be in sync with Check()
			volatile ULONG_PTR m_ulpRefs;
			
#ifdef GPOS_DEBUG
			// sanity check to detect deleted memory
			void Check() 
			{
				// assert that first member of class has not been wiped
				GPOS_ASSERT(m_ulpRefs != GPOS_WIPED_MEM_PATTERN);
			}
#endif // GPOS_DEBUG

			// private copy ctor
			CRefCount(const CRefCount &);

		public:
		
			// ctor
			CRefCount() 
				: 
				m_ulpRefs(1)
			{}

			// dtor
			virtual ~CRefCount()
			{
				// enforce strict ref-counting unless we're in a pending exception,
				// e.g., a ctor has thrown
				GPOS_ASSERT(NULL == ITask::PtskSelf() ||
							ITask::PtskSelf()->FPendingExc() ||
							0 == m_ulpRefs);
			}

			// return ref-count
			ULONG_PTR UlpRefCount() const
			{
				return m_ulpRefs;
			}

			// return true if calling object's destructor is allowed
			virtual
			BOOL FDeletable() const
			{
				return true;
			}

			// count up
			void AddRef()
			{
#ifdef GPOS_DEBUG
				Check();
#endif // GPOS_DEBUG				
				(void) UlpExchangeAdd(&m_ulpRefs, 1);
			}

			// count down
			void Release()
			{
#ifdef GPOS_DEBUG	
				Check();
#endif // GPOS_DEBUG
				if (1 == UlpExchangeAdd(&m_ulpRefs, -1))
				{
					// the following check is not thread-safe -- we intentionally allow this to capture
					// the exceptional case where ref-count wrongly reaching zero
					if (!FDeletable())
					{
						// restore ref-count
						AddRef();

						// deletion is not allowed
						GPOS_RAISE(CException::ExmaSystem, CException::ExmiInvalidDeletion);
					}

					GPOS_DELETE(this);
				}	
			}

			// safe version of Release -- handles NULL pointers
			static
			void SafeRelease
				(
				CRefCount *prc
				)
			{
				if (NULL != prc)
				{
					prc->Release();
				}
			}
	
	}; // class CRefCount
}

#endif // !GPOS_CRefCount_H

// EOF

