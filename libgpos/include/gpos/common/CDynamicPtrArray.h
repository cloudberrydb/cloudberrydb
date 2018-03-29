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
#include "gpos/common/clibwrapper.h"

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
			static INT PtrCmp(const void *pv1, const void *pv2)
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

			// private copy ctor
			CDynamicPtrArray<T, pfnDestroy> (const CDynamicPtrArray<T, pfnDestroy> &);
			
			// resize function
			void Resize(ULONG ulNewSize)
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
			
		public:
		
			// ctor
			explicit
			CDynamicPtrArray<T, pfnDestroy> (IMemoryPool *pmp, ULONG ulMinSize = 4, ULONG ulExp = 10)
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

			// dtor
			~CDynamicPtrArray<T, pfnDestroy> ()
            {
                Clear();

                GPOS_DELETE_ARRAY(m_ppt);
            }
	
			// clear elements
			void Clear()
            {
                for(ULONG i = 0; i < m_ulSize; i++)
                {
                    pfnDestroy(m_ppt[i]);
                }
                m_ulSize = 0;
            }
	
			// append element to end of array
			void Append(T *pt)
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
			
			// append array -- flatten it
			void AppendArray(const CDynamicPtrArray<T, pfnDestroy> *pdrg)
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

			
			// number of elements currently held
			ULONG UlLength() const
            {
                return m_ulSize;
            }

			// sort array
			void Sort(PfnCompare pfncompare = PtrCmp)
            {
                clib::QSort(m_ppt, m_ulSize, sizeof(T*), pfncompare);
            }
			
			// equality check
			BOOL FEqual(const CDynamicPtrArray<T, pfnDestroy> *pdrg) const
            {
                BOOL fEqual = (UlLength() == pdrg->UlLength());

                for (ULONG i = 0; i < m_ulSize && fEqual; i++)
                {
                    fEqual = (m_ppt[i] == pdrg->m_ppt[i]);
                }

                return fEqual;
            }
			
			// lookup object
			T* PtLookup(const T *pt) const
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

			// lookup object position
            ULONG
            UlPos(const T *pt) const
            {
                GPOS_ASSERT(NULL != pt);

                for (ULONG ul = 0; ul < m_ulSize; ul++)
                {
                    if (*m_ppt[ul] == *pt)
                    {
                        return ul;
                    }
                }

                return gpos::ulong_max;
            }

#ifdef GPOS_DEBUG
			// check if array is sorted
			BOOL FSorted() const
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
						
			// accessor for n-th element
			T *operator [] (ULONG ulPos) const
            {
                GPOS_ASSERT(ulPos < m_ulSize && "Out of bounds access");
                return (T*) m_ppt[ulPos];
            }
			
			// replace an element in the array
			void  Replace(ULONG ulPos, T *pt)
            {
                GPOS_ASSERT(ulPos < m_ulSize && "Out of bounds access");
                pfnDestroy(m_ppt[ulPos]);
                m_ppt[ulPos] = pt;
            }
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

	// arrays of chars
	typedef CDynamicPtrArray<CHAR, CleanupDelete> DrgPsz;

}

#endif // !GPOS_CDynamicPtrArray_H

// EOF

