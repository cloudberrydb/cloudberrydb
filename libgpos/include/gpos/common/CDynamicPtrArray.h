//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CDynamicPtrArray.h
//
//	@doc:
//		Dynamic array of pointers frequently used in optimizer data structures
//---------------------------------------------------------------------------
#ifndef GPOS_CDynamicPtrArray_H
#define GPOS_CDynamicPtrArray_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

namespace gpos
{
	
	// comparison function signature
	typedef INT (*PfnCompare)(const void *, const void *);

	//---------------------------------------------------------------------------
	//	@class:
	//		CDynamicPtrArray
	//
	//	@doc:
	//		Simply dynamic array for pointer types
	//
	//---------------------------------------------------------------------------
	template <class T, void (*pfnDestroy)(T*)>
	class CDynamicPtrArray : public CRefCount
	{
		private:
		
			// memory pool
			IMemoryPool *m_pmp;
			
			// currently allocated size
			ULONG m_ulAllocated;
			
			// min size
			ULONG m_ulMinSize;
			
			// current size
			ULONG m_ulSize;

			// expansion factor
			ULONG m_ulExp;

			// actual array
			T **m_ppt;
			
			// comparison function for pointers
			static INT PtrCmp(const void *, const void *);

			// private copy ctor
			CDynamicPtrArray<T, pfnDestroy> (const CDynamicPtrArray<T, pfnDestroy> &);
			
			// resize function
			void Resize(ULONG);
			
		public:
		
			// ctor
			explicit
			CDynamicPtrArray<T, pfnDestroy> (IMemoryPool*, ULONG = 4, ULONG = 10);

			// dtor
			~CDynamicPtrArray<T, pfnDestroy> ();
	
			// clear elements
			void Clear();
	
			// append element to end of array
			void Append(T*);
			
			// append array -- flatten it
			void AppendArray(const CDynamicPtrArray<T, pfnDestroy> *);
			
			// number of elements currently held
			ULONG UlLength() const;

			// safe version of length -- handles NULL pointers
			ULONG UlSafeLength() const;
			
			// sort array
			void Sort(PfnCompare pfncompare = PtrCmp);
			
			// equality check
			BOOL FEqual(const CDynamicPtrArray<T, pfnDestroy> *) const;
			
			// lookup object
			T* PtLookup(const T*) const;
			
			// lookup object position
			ULONG UlPos(const T*) const;

#ifdef GPOS_DEBUG
			// check if array is sorted
			BOOL FSorted() const;
#endif // GPOS_DEBUG
						
			// accessor for n-th element
			T *operator [] (ULONG) const;
			
			// replace an element in the array
			void  Replace(ULONG ulPos, T *pt);
	}; // class CDynamicPtrArray
		

	// frequently used destroy functions
	
	// NOOP 
	template<class T>
	inline void CleanupNULL(T*) {}
	
	// plain delete
	template<class T>
	inline void CleanupDelete(T *pt)
	{
		GPOS_DELETE(pt);
	}

	// delete of array
	template<class T>
	inline void CleanupDeleteRg(T *pt)
	{
		GPOS_DELETE_ARRAY(pt);
	}

	// release ref-count'd object
	template<class T>
	inline void CleanupRelease(T *pt)
	{
		(dynamic_cast<CRefCount*>(pt))->Release();
	}

	// commonly used array types

	// arrays of unsigned integers
	typedef CDynamicPtrArray<ULONG, CleanupDelete> DrgPul;
	// array of unsigned integer arrays
	typedef CDynamicPtrArray<DrgPul, CleanupRelease> DrgPdrgPul;

	// arrays of integers
	typedef CDynamicPtrArray<INT, CleanupDelete> DrgPi;

	// array of strings
	typedef CDynamicPtrArray<CWStringBase, CleanupDelete> DrgPstr;

}

#include "CDynamicPtrArray.inl"

#endif // !GPOS_CDynamicPtrArray_H

// EOF

