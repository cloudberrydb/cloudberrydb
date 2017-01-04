//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CDynamicPtrArray.inl
//
//	@doc:
//		Implementation of inlined functions;
//---------------------------------------------------------------------------
#ifndef GPOS_CDynamicPtrArray_INL
#define GPOS_CDynamicPtrArray_INL

#include "gpos/common/clibwrapper.h"

namespace gpos
{
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::CDynamicPtrArray
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	CDynamicPtrArray<T, pfnDestroy>::CDynamicPtrArray
		(
		IMemoryPool *pmp,
		ULONG ulMinSize,
		ULONG ulExp
		)
		: 
			m_pmp(pmp),
			m_ulAllocated(0),
			m_ulMinSize(std::max((ULONG)4, ulMinSize)),
			m_ulSize(0),
			m_ulExp(std::max((ULONG)2, ulExp)),
			m_ppt(NULL)
	{
		GPOS_ASSERT(NULL != pfnDestroy && "No valid destroy function specified");
		
		// do not allocate in constructor; defer allocation to first insertion
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::~CDynamicPtrArray
	//
	//	@doc:
	//		dtor
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	CDynamicPtrArray<T, pfnDestroy>::~CDynamicPtrArray ()
	{
		Clear();
		
		GPOS_DELETE_ARRAY(m_ppt);

	}
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::Clear
	//
	//	@doc:
	//		execute destructor function on all elements then reset current 
	//		size counter; do not delete memory
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	void
	CDynamicPtrArray<T, pfnDestroy>::Clear ()
	{
		for(ULONG i = 0; i < m_ulSize; i++)
		{
			pfnDestroy(m_ppt[i]);
		}
		m_ulSize = 0;
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::Resize
	//
	//	@doc:
	//		Resize array with given new target size
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	void
	CDynamicPtrArray<T, pfnDestroy>::Resize
		(
		ULONG ulNewSize
		)
	{
		GPOS_ASSERT(ulNewSize > m_ulAllocated && "Invalid call to Resize, cannot shrink array");
		
		// get new target array
		T **ppt = GPOS_NEW_ARRAY(m_pmp, T*, ulNewSize);
		
		if (m_ulSize > 0)
		{
			GPOS_ASSERT(NULL != m_ppt);
			
			clib::PvMemCpy(ppt, m_ppt, sizeof(T*) * m_ulSize);

			GPOS_DELETE_ARRAY(m_ppt);
		}
		
		m_ppt = ppt;
		
		m_ulAllocated = ulNewSize;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::Append
	//
	//	@doc:
	//		Add a given pointer to the array, resize as necessary
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	void
	CDynamicPtrArray<T, pfnDestroy>::Append
		(
		T *pt
		)
	{		
		if (m_ulSize == m_ulAllocated)
		{
			// resize at least by 4 elements or percentage as given by ulExp
			ULONG ulExpand = (ULONG) (m_ulAllocated * (1 + (m_ulExp/100.0)));
			ULONG ulMinExpand = m_ulAllocated + 4;

			Resize(std::max(std::max(ulMinExpand, ulExpand), m_ulMinSize));
		}
		
		GPOS_ASSERT(m_ulSize < m_ulAllocated);
				
		m_ppt[m_ulSize] = pt;
		++m_ulSize;
	}



	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::AppendArray
	//
	//	@doc:
	//		Add a another array, resize as necessary
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	void
	CDynamicPtrArray<T, pfnDestroy>::AppendArray
		(
		const CDynamicPtrArray<T, pfnDestroy> *pdrg
		)
	{
		GPOS_ASSERT(NULL != pdrg);
		GPOS_ASSERT(this != pdrg && "Cannot append array to itself");
	
		ULONG ulTotalSize = m_ulSize + pdrg->m_ulSize;
		if (ulTotalSize > m_ulAllocated)
		{
			Resize(ulTotalSize);
		}
		
		GPOS_ASSERT(m_ulSize <= m_ulAllocated);
		GPOS_ASSERT_IMP(m_ulSize == m_ulAllocated, 0 == pdrg->m_ulSize);
		
		GPOS_ASSERT(ulTotalSize <= m_ulAllocated);
				
		// at this point old memory is no longer accessible, hence, no self-copy
		if (pdrg->m_ulSize > 0)
		{
			GPOS_ASSERT(NULL != pdrg->m_ppt);
			clib::PvMemCpy(m_ppt + m_ulSize, pdrg->m_ppt, pdrg->m_ulSize * sizeof(T*));
		}
		
		m_ulSize = ulTotalSize;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::UlLength
	//
	//	@doc:
	//		Determine current size of array
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	ULONG
	CDynamicPtrArray<T, pfnDestroy>::UlLength() const
	{
		return m_ulSize;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::PtrCmp
	//
	//	@doc:
	//		Comparison function for quick-sort
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	INT
	CDynamicPtrArray<T, pfnDestroy>::PtrCmp
		(
		const void *pv1, 
		const void *pv2
		)
	{
		ULONG_PTR ulp1 = *(ULONG_PTR*)pv1;
		ULONG_PTR ulp2 = *(ULONG_PTR*)pv2;
	
		if (ulp1 < ulp2)
		{
			return -1;
		}
		
		if (ulp1 > ulp2)
		{
			return 1;
		}
		
		return 0;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::Sort
	//
	//	@doc:
	//		Quick-sort the array
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	void
	CDynamicPtrArray<T, pfnDestroy>::Sort(PfnCompare pfncompare)
	{
		clib::QSort(m_ppt, m_ulSize, sizeof(T*), pfncompare);
	}

#ifdef GPOS_DEBUG
	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::FSorted
	//
	//	@doc:
	//		Check if array is sorted
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	BOOL
	CDynamicPtrArray<T, pfnDestroy>::FSorted() const
	{
		for (ULONG i = 1; i < m_ulSize; i++)
		{
			if ((ULONG_PTR)(m_ppt[i - 1]) > (ULONG_PTR)(m_ppt[i]))
			{
				return false;
			}
		}
		
		return true;
	}
#endif // GPOS_DEBUG


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::FEqual
	//
	//	@doc:
	//		Check if arrays are equal
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	BOOL
	CDynamicPtrArray<T, pfnDestroy>::FEqual
		(
		const CDynamicPtrArray<T, pfnDestroy> *pdrg
		)
		const
	{
		BOOL fEqual = (UlLength() == pdrg->UlLength());
		
		for (ULONG i = 0; i < m_ulSize && fEqual; i++)
		{
			fEqual = (m_ppt[i] == pdrg->m_ppt[i]);
		}
		
		return fEqual;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::PtLookup
	//
	//	@doc:
	//		Check if given element is contained in array
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	T*
	CDynamicPtrArray<T, pfnDestroy>::PtLookup
		(
		const T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		
		for (ULONG i = 0; i < m_ulSize; i++)
		{
			if (*m_ppt[i] == *pt)
			{
				return m_ppt[i];
			}
		}
		
		return NULL;
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::UlPos
	//
	//	@doc:
	//		Lookup position of given element is in the array
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	ULONG
	CDynamicPtrArray<T, pfnDestroy>::UlPos
		(
		const T *pt
		)
		const
	{
		GPOS_ASSERT(NULL != pt);
		
		for (ULONG ul = 0; ul < m_ulSize; ul++)
		{
			if (*m_ppt[ul] == *pt)
			{
				return ul;
			}
		}
		
		return ULONG_MAX;
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::UlSafeLength
	//
	//	@doc:
	//		Determine size of given array; allow for NULL pointers
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	ULONG
	CDynamicPtrArray<T, pfnDestroy>::UlSafeLength() const
	{
		if (NULL == this)
		{
			return 0;
		}
		else
		{
			return UlLength();
		}
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::operator[](ULONG)
	//
	//	@doc:
	//		Accessor for n-th element of array
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	T *
	CDynamicPtrArray<T, pfnDestroy>::operator [] 
		(
		ULONG ulPos
		)
	const
	{
		GPOS_ASSERT(ulPos < m_ulSize && "Out of bounds access");
		return (T*) m_ppt[ulPos];
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArray::Replace
	//
	//	@doc:
	//		Replace an element of the array. The cleanup function is called on the 
	//		replaced element.
	//
	//---------------------------------------------------------------------------
	template<class T, void (*pfnDestroy)(T*)>
	void
	CDynamicPtrArray<T, pfnDestroy>::Replace 
		(
		ULONG ulPos,
		T *pt
		)
	{
		GPOS_ASSERT(ulPos < m_ulSize && "Out of bounds access");
		pfnDestroy(m_ppt[ulPos]);
		m_ppt[ulPos] = pt;
	}

}

#endif // !GPOS_CDynamicPtrArray_INL

// EOF
