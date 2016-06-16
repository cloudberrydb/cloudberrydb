//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CStack.inl
//
//	@doc:
//		Implementation of inlined stack functions;
//---------------------------------------------------------------------------
#ifndef GPOS_CStack_INL
#define GPOS_CStack_INL

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@function:
	//		CStack::CStack
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template<class T>
	CStack<T>::CStack
		(
		IMemoryPool *pmp,
		ULONG ulMinSize
		)
		: m_ulSize(0)
	{
		m_dpa = GPOS_NEW(pmp) CDynamicPtrArray<T, CleanupNULL>(pmp, ulMinSize, 10);
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CStack::Push
	//
	//	@doc:
	//		Add an element to the top of the stack
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CStack<T>::Push
		(
		T *pt
		)
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


	//---------------------------------------------------------------------------
	//	@function:
	//		CStack::Pop
	//
	//	@doc:
	//		Remove element from top of the stack
	//
	//---------------------------------------------------------------------------
	template<class T>
	T*
	CStack<T>::Pop
		(
		)
	{		
		GPOS_ASSERT(!this->FEmpty() && "Cannot pop from empty stack");

		T *pt = (*m_dpa)[m_ulSize - 1];
		m_ulSize--;
		return pt;		
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CStack::Peek
	//
	//	@doc:
	//		Peek at top element in stack
	//
	//---------------------------------------------------------------------------
	template<class T>
	const T*
	CStack<T>::Peek
		(
		) const
	{		
		GPOS_ASSERT(!this->FEmpty() && "Cannot peek into empty stack");

		const T *pt = (*m_dpa)[m_ulSize - 1];

		return pt;		
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CStack::FEmpty()
	//
	//	@doc:
	//		Is the stack empty?
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL 
	CStack<T>::FEmpty
		(
		) const
	{		
		return (m_ulSize == 0);
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CStack::~CStack()
	//
	//	@doc:
	//		Destructor
	//
	//---------------------------------------------------------------------------
	template<class T>
	CStack<T>::~CStack
		(
		)
	{		
		m_dpa->Release();
	}
		
		
}

#endif
