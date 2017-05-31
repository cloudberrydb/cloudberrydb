//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CStack.h
//
//	@doc:
//		Create stack of objects
//
//	@owner: 
//		Siva
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CStack_H
#define GPOPT_CStack_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{

	template <class T>	
	class CStack
	{
		private:	
			// backing dynamic array store
			CDynamicPtrArray<T, CleanupNULL> *m_dpa; 
			
			// top of stack index
			ULONG m_ulSize;
			
			// copy c'tor - not defined
			CStack(CStack &);
			
		public:
			
			// c'tor
			CStack<T> (IMemoryPool *pmp, ULONG ulMinSize = 4)
            : m_ulSize(0)
            {
                m_dpa = GPOS_NEW(pmp) CDynamicPtrArray<T, CleanupNULL>(pmp, ulMinSize, 10);
            }
			
			// destructor 
			virtual ~CStack()
            {
                m_dpa->Release();
            }
						
			// push element onto stack
			void Push(T *pt)
            {
                GPOS_ASSERT(m_dpa != NULL && "Dynamic array missing");
                GPOS_ASSERT(m_ulSize <= m_dpa->UlLength() && "The top of stack cannot be beyond the underlying array");

                // if the stack was Popped before, reuse that space by replacing the element
                if (m_ulSize < m_dpa->UlLength())
                {
                    m_dpa->Replace(m_ulSize, pt);
                }
                else
                {
                    m_dpa->Append(pt);
                }

                m_ulSize++;
            }
						
			// pop top element
			T*	Pop()
            {
                GPOS_ASSERT(!this->FEmpty() && "Cannot pop from empty stack");

                T *pt = (*m_dpa)[m_ulSize - 1];
                m_ulSize--;
                return pt;
            }
			
			// peek at top element
			const T* Peek() const
            {
                GPOS_ASSERT(!this->FEmpty() && "Cannot peek into empty stack");

                const T *pt = (*m_dpa)[m_ulSize - 1];

                return pt;
            }
			
			// is stack empty?
			BOOL FEmpty() const
            {
                return (m_ulSize == 0);
            }
	};
	
}

#endif // !GPOPT_CStack_H

// EOF

