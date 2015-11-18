//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CHashSet.h
//
//	@doc:
//		Hash set
//		* stores deep objects, i.e., pointers
//		* equality == on objects uses template function argument
//		* does not allow insertion of duplicates
//		* destroys objects based on client-side provided destroy functions
//
//	@owner:
//		solimm1
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CHashSet_H
#define GPOS_CHashSet_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CHashSet
	//
	//	@doc:
	//		Hash set
	//
	//---------------------------------------------------------------------------
	template <class T,
				ULONG (*pfnHash)(const T*),
				BOOL (*pfnEq)(const T*, const T*),
				void (*pfnDestroy)(T*)>
	class CHashSet : public CRefCount
	{

		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		CHashSetElem
			//
			//	@doc:
			//		Anchor for set element
			//
			//---------------------------------------------------------------------------
			class CHashSetElem
			{
				private:

					// pointer to object
					T *m_pt;

					// does hash set element own object?
					BOOL m_fOwn;

					// private copy ctor
					CHashSetElem(const CHashSetElem &);

				public:

					// ctor
					CHashSetElem(T *pt, BOOL fOwn);

					// dtor
					~CHashSetElem();

					// object accessor
					T *Pt() const
					{
						return m_pt;
					}

					// equality operator
					BOOL operator == (const CHashSetElem &hse) const
					{
						return pfnEq(m_pt, hse.m_pt);
					}
			};	// class CHashSetElem

			// memory pool
			IMemoryPool *m_pmp;

			// number of hash chains
			ULONG m_ulSize;

			// total number of entries
			ULONG m_ulEntries;

			// each hash chain is an array of hashset elements
			typedef CDynamicPtrArray<CHashSetElem, CleanupDelete> DrgHashChain;
			DrgHashChain **m_ppdrgchain;

			// private copy ctor
			CHashSet(const CHashSet<T, pfnHash, pfnEq, pfnDestroy> &);

			// lookup appropriate hash chain in static table, may be NULL if
			// no elements have been inserted yet
			DrgHashChain **PpdrgChain(const T *pt) const
			{
				GPOS_ASSERT(NULL != m_ppdrgchain);
				return &m_ppdrgchain[pfnHash(pt) % m_ulSize];
			}

			// clear elements
			void Clear();

		public:

			// ctor
			CHashSet<T, pfnHash, pfnEq, pfnDestroy> (IMemoryPool *pmp, ULONG ulSize = 128);

			// dtor
			~CHashSet<T, pfnHash, pfnEq, pfnDestroy> ();

			// insert an element if not present
			BOOL FInsert(T *pt);

			// lookup element
			BOOL FExists(const T *pt) const;

			// return number of map entries
			ULONG UlEntries() const
			{
				return m_ulEntries;
			}

	}; // class CHashSet

}

// inline'd functions
#include "CHashSet.inl"

#endif // !GPOS_CHashSet_H

// EOF

