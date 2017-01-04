//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CHashMap.h
//
//	@doc:
//		Hash map
//		* stores deep objects, i.e., pointers
//		* equality == on key uses template function argument
//		* does not allow insertion of duplicates (no equality on value class req'd)
//		* destroys objects based on client-side provided destroy functions
//---------------------------------------------------------------------------
#ifndef GPOS_CHashMap_H
#define GPOS_CHashMap_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{	
	// fwd declaration
	template <class K, class T, 
		ULONG (*pfnHash)(const K*), 
		BOOL (*pfnEq)(const K*, const K*),
		void (*pfnDestroyK)(K*),
		void (*pfnDestroyT)(T*)>
	class CHashMapIter;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CHashMap
	//
	//	@doc:
	//		Hash map
	//
	//---------------------------------------------------------------------------
	template <class K, class T, 
				ULONG (*pfnHash)(const K*), 
				BOOL (*pfnEq)(const K*, const K*),
				void (*pfnDestroyK)(K*),
				void (*pfnDestroyT)(T*)>
	class CHashMap : public CRefCount
	{
		// fwd declaration
		friend class CHashMapIter<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT>;

		private:
		
			//---------------------------------------------------------------------------
			//	@class:
			//		CHashMapElem
			//
			//	@doc:
			//		Anchor for key/value pair
			//
			//---------------------------------------------------------------------------		
			class CHashMapElem
			{
				private:
				
					// key/value pair
					K *m_pk;
					T *m_pt;
					
					// own objects
					BOOL m_fOwn;
					
					// private copy ctor
					CHashMapElem(const CHashMapElem &);
				
				public:
				
					// ctor
					CHashMapElem(K *pk, T *pt, BOOL fOwn);

					// dtor 
					~CHashMapElem();

					// key accessor
					K *Pk() const
					{
						return m_pk;
					}

					// value accessor
					T *Pt() const
					{
						return m_pt;
					}
					
					// replace value
					void ReplaceValue(T *ptNew);

					// equality operator -- map elements are equal if their keys match
					BOOL operator == (const CHashMapElem &hme) const
					{
						return pfnEq(m_pk, hme.m_pk);
					}
			};

			// memory pool
			IMemoryPool *m_pmp;
			
			// size
			ULONG m_ulSize;
		
			// number of entries
			ULONG m_ulEntries;
		
			// each hash chain is an array of hashmap elements
			typedef CDynamicPtrArray<CHashMapElem, CleanupDelete> DrgHashChain;
			DrgHashChain **m_ppdrgchain;

			// private copy ctor
			CHashMap(const CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> &);
			
			// lookup appropriate hash chain in static table, may be NULL if
			// no elements have been inserted yet
			DrgHashChain **PpdrgChain(const K *pk) const
			{
				GPOS_ASSERT(NULL != m_ppdrgchain);
				return &m_ppdrgchain[pfnHash(pk) % m_ulSize];
			}

			// clear elements
			void Clear();
	
			// lookup an element by its key
			void Lookup(const K *pk, CHashMapElem **pphme) const;

		public:
		
			// ctor
			CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> (IMemoryPool *pmp, ULONG ulSize = 128);

			// dtor
			~CHashMap<K, T, pfnHash, pfnEq, pfnDestroyK, pfnDestroyT> ();

			// insert an element if key is not yet present
			BOOL FInsert(K *pk, T *pt);
			
			// lookup a value by its key
			T *PtLookup(const K *pk) const;

			// replace the value in a map entry with a new given value
			BOOL FReplace(const K *pk, T *ptNew);
		
			// return number of map entries
			ULONG UlEntries() const
			{
				return m_ulEntries;
			}		

	}; // class CHashMap

}

// inline'd functions
#include "CHashMap.inl"

#endif // !GPOS_CHashMap_H

// EOF

