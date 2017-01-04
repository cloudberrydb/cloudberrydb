//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBitSet.h
//
//	@doc:
//		Implementation of bitset as linked list of bitvectors
//---------------------------------------------------------------------------
#ifndef GPOS_CBitSet_H
#define GPOS_CBitSet_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CBitVector.h"
#include "gpos/common/CList.h"


namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CBitSet
	//
	//	@doc:
	//		Linked list of CBitSetLink's
	//
	//---------------------------------------------------------------------------
	class CBitSet : public CRefCount
	{
		// bitset iter needs to access internals
		friend class CBitSetIter;
		
		protected:

			//---------------------------------------------------------------------------
			//	@class:
			//		CBitSetLink
			//
			//	@doc:
			//		bit vector + offset + link
			//
			//---------------------------------------------------------------------------
			class CBitSetLink
			{
				private:
				
					// private copy ctor
					CBitSetLink(const CBitSetLink &);

					// offset
					ULONG m_ulOffset;
					
					// bitvector
					CBitVector *m_pbv;

				public:
				
					// ctor
                    explicit
                    CBitSetLink(IMemoryPool *, ULONG ulOffset, ULONG cSizeBits);

        			explicit
					CBitSetLink(IMemoryPool *, const CBitSetLink &);
					
					// dtor
					~CBitSetLink();

					// accessor
					ULONG UlOffset() const
					{
						return m_ulOffset;
					}
					
					// accessor
					CBitVector *Pbv() const
					{
						return m_pbv;
					}
										
					// list link
					SLink m_link;
					
			}; // class CBitSetLink
		
			// list of bit set links
			typedef CList<CBitSetLink> BSLList;
			BSLList m_bsllist;
		
			// pool to allocate links from
			IMemoryPool *m_pmp;
		
			// size of individual bitvectors
			ULONG m_cSizeBits;
			
			// number of elements
			ULONG m_cElements;
		
			// private copy ctor
			CBitSet(const CBitSet&);
			
			// find link with offset less or equal to given value
			CBitSetLink *PbslLocate(ULONG, CBitSetLink * = NULL) const;
			
			// reset set
			void Clear();
			
			// compute target offset
			ULONG UlOffset(ULONG) const;
			
			// re-compute size of set
			void RecomputeSize();
			
		public:
				
			// ctor
			CBitSet(IMemoryPool *pmp, ULONG cSizeBits = 256);
			CBitSet(IMemoryPool *pmp, const CBitSet &);
			
			// dtor
			virtual ~CBitSet();
			
			// determine if bit is set
			BOOL FBit(ULONG ulBit) const;
			
			// set given bit; return previous value
			BOOL FExchangeSet(ULONG ulBit);
						
			// clear given bit; return previous value
			BOOL FExchangeClear(ULONG ulBit);
			
			// union sets
			void Union(const CBitSet *);
			
			// intersect sets
			void Intersection(const CBitSet *);
			
			// difference of sets
			void Difference(const CBitSet *);
			
			// is subset
			BOOL FSubset(const CBitSet *) const;
			
			// equality
			BOOL FEqual(const CBitSet *) const;
			
			// disjoint
			BOOL FDisjoint(const CBitSet *) const;
			
			// hash value for set
			ULONG UlHash() const;
			
			// number of elements
			ULONG CElements() const
			{
				return m_cElements;
			}
			
			// print function
			IOstream &OsPrint(IOstream &os) const;

	}; // class CBitSet


	// shorthand for printing
	inline
	IOstream &operator << 
		(
		IOstream &os, 
		CBitSet &bs
		)
	{
		return bs.OsPrint(os);
	}
}

#endif // !GPOS_CBitSet_H

// EOF

