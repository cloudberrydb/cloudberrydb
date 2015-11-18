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
			CStack<T> (IMemoryPool *pmp, ULONG ulMinSize = 4);
			
			// destructor 
			virtual ~CStack();
						
			// push element onto stack
			void Push(T *);
						
			// pop top element
			T*	Pop();
			
			// peek at top element
			const T* Peek() const;
			
			// is stack empty?
			BOOL FEmpty() const;
	};
	
}

#include "CStack.inl"

#endif // !GPOPT_CStack_H

// EOF

