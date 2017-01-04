//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CBitVector.h
//
//	@doc:
//		Implementation of static bit vector;
//---------------------------------------------------------------------------
#ifndef GPOS_CBitVector_H
#define GPOS_CBitVector_H

#include "gpos/base.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CBitVector
	//
	//	@doc:
	//		Bit vector based on ULLONG elements
	//
	//---------------------------------------------------------------------------
	class CBitVector
	{
		private:
		
			// size in bits
			ULONG m_cBits;
		
			// size of vector in units, not bits
			ULONG m_cUnits;
		
			// vector
			ULLONG *m_rgull;
		
			// no default copy ctor
			CBitVector(const CBitVector&);
			
			// clear vector
			void Clear();
			
		public:
				
			// ctor
			CBitVector(IMemoryPool *pmp, ULONG cBits);
			
			// dtor
			~CBitVector();
			
			// copy ctor with target mem pool
			CBitVector(IMemoryPool *pmp, const CBitVector &);
			
			// determine if bit is set
			BOOL FBit(ULONG ulBit) const;
			
			// set given bit; return previous value
			BOOL FExchangeSet(ULONG ulBit);
						
			// clear given bit; return previous value
			BOOL FExchangeClear(ULONG ulBit);
			
			// union vectors
			void Union(const CBitVector *);
			
			// intersect vectors
			void Intersection(const CBitVector *);
			
			// is subset
			BOOL FSubset(const CBitVector *) const;
			
			// is dijoint
			BOOL FDisjoint(const CBitVector *) const;
			
			// equality
			BOOL FEqual(const CBitVector *) const;
			
			// is empty?
			BOOL FEmpty() const;
			
			// find next bit from given position
			BOOL FNextBit(ULONG, ULONG&) const;

			// number of bits set
			ULONG CElements() const;
			
			// hash value
			ULONG UlHash() const;

	}; // class CBitVector

}

#endif // !GPOS_CBitVector_H

// EOF

