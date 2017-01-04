//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CBitVector.cpp
//
//	@doc:
//		Implementation of simple, static bit vector class
//---------------------------------------------------------------------------


#include "gpos/base.h"
#include "gpos/utils.h"
#include "gpos/common/CBitVector.h"
#include "gpos/common/CAutoRg.h"
#include "gpos/common/clibwrapper.h"

using namespace gpos;

#define BYTES_PER_UNIT	GPOS_SIZEOF(ULLONG)
#define BITS_PER_UNIT	(8 * BYTES_PER_UNIT)

//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Clear
//
//	@doc:
//		wipe all units
//
//---------------------------------------------------------------------------
void
CBitVector::Clear()
{
	GPOS_ASSERT(NULL != m_rgull);
	clib::PvMemSet(m_rgull, 0, m_cUnits * BYTES_PER_UNIT);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::CBitVector
//
//	@doc:
//		ctor -- allocates actual vector, clears it
//
//---------------------------------------------------------------------------
CBitVector::CBitVector
	(
	IMemoryPool *pmp,
	ULONG cBits
	)
	:
	m_cBits(cBits),
	m_cUnits(0),
	m_rgull(NULL)
{
	// determine units needed to represent the number
	m_cUnits = m_cBits / BITS_PER_UNIT;
	if (m_cUnits * BITS_PER_UNIT < m_cBits)
	{
		m_cUnits++;
	}
	
	GPOS_ASSERT(m_cUnits * BITS_PER_UNIT >= m_cBits && "Bit vector sized incorrectly");
	
	// allocate and clear
	m_rgull = GPOS_NEW_ARRAY(pmp, ULLONG, m_cUnits);
	
	CAutoRg<ULLONG> argull;
	argull = m_rgull;
	
	Clear();
	
	// unhook from protector
	argull.RgtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::~CBitVector
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CBitVector::~CBitVector()
{
	GPOS_DELETE_ARRAY(m_rgull);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::CBitVector
//
//	@doc:
//		copy ctor;
//
//---------------------------------------------------------------------------
CBitVector::CBitVector
	(
	IMemoryPool *pmp,
	const CBitVector &bv
	)
	:
	m_cBits(bv.m_cBits),
	m_cUnits(bv.m_cUnits),
	m_rgull(NULL)
{
	
	// deep copy
	m_rgull = GPOS_NEW_ARRAY(pmp, ULLONG, m_cUnits);
	
	// Using auto range for cleanliness only;
	// NOTE: 03/25/2008; strictly speaking not necessary since there is
	//		no operation that could fail and it's the only allocation in the 
	//		ctor; 
	CAutoRg<ULLONG> argull;
	argull = m_rgull;
	
	clib::PvMemCpy(m_rgull, bv.m_rgull, BYTES_PER_UNIT * m_cUnits);

	// unhook from protector
	argull.RgtReset();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FBit
//
//	@doc:
//		Check if given bit is set
//
//---------------------------------------------------------------------------
BOOL 
CBitVector::FBit
	(
	ULONG ulBit
	)
	const
{
	GPOS_ASSERT(ulBit < m_cBits && "Bit index out of bounds.");

	ULONG cUnit = ulBit / BITS_PER_UNIT;
	ULLONG ullMask = ((ULLONG)1) << (ulBit % BITS_PER_UNIT);
	
	return m_rgull[cUnit] & ullMask;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FExchangeSet
//
//	@doc:
//		Set given bit; return previous value
//
//---------------------------------------------------------------------------
BOOL 
CBitVector::FExchangeSet
	(
	ULONG ulBit
	)
{
	GPOS_ASSERT(ulBit < m_cBits  && "Bit index out of bounds.");

	// CONSIDER: 03/25/2008; make testing for the bit part of this routine and
	// avoid function call
	BOOL fSet = FBit(ulBit);
	
	ULONG cUnit = ulBit / BITS_PER_UNIT;
	ULLONG ullMask = ((ULLONG)1) << (ulBit % BITS_PER_UNIT);
	
	// OR the target unit with the mask
	m_rgull[cUnit] |= ullMask;
	
	return fSet;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FExchangeClear
//
//	@doc:
//		Clear given bit; return previous value
//
//---------------------------------------------------------------------------
BOOL 
CBitVector::FExchangeClear
	(
	ULONG ulBit
	)
{
	GPOS_ASSERT(ulBit < m_cBits && "Bit index out of bounds.");

	// CONSIDER: 03/25/2008; make testing for the bit part of this routine and
	// avoid function call
	BOOL fSet = FBit(ulBit);
	
	ULONG cUnit = ulBit / BITS_PER_UNIT;
	ULLONG ullMask = ((ULLONG)1) << (ulBit % BITS_PER_UNIT);
	
	// AND the target unit with the inverted mask
	m_rgull[cUnit] &= ~ullMask;
	
	return fSet;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Union
//
//	@doc:
//		Union with given other vector
//
//---------------------------------------------------------------------------
void
CBitVector::Union
	(
	const CBitVector *pbv
	)
{
	GPOS_ASSERT(m_cBits == pbv->m_cBits && m_cUnits == pbv->m_cUnits && 
		"vectors must be of same size");
		
	// OR all components
	for(ULONG i = 0; i < m_cUnits; i++)
	{
		m_rgull[i] |= pbv->m_rgull[i];
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::Intersection
//
//	@doc:
//		Intersect with given other vector
//
//---------------------------------------------------------------------------
void
CBitVector::Intersection
	(
	const CBitVector *pbv
	)
{
	GPOS_ASSERT(m_cBits == pbv->m_cBits && m_cUnits == pbv->m_cUnits && 
		"vectors must be of same size");
		
	// AND all components
	for(ULONG i = 0; i < m_cUnits; i++)
	{
		m_rgull[i] &= pbv->m_rgull[i];
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FSubset
//
//	@doc:
//		Determine if given vector is subset
//
//---------------------------------------------------------------------------
BOOL
CBitVector::FSubset
	(
	const CBitVector *pbv
	)
	const
{
	GPOS_ASSERT(m_cBits == pbv->m_cBits && m_cUnits == pbv->m_cUnits && 
		"vectors must be of same size");
		
	// OR all components
	for(ULONG i = 0; i < m_cUnits; i++)
	{
		ULLONG ull = m_rgull[i] & pbv->m_rgull[i];
		if (ull != pbv->m_rgull[i])
		{
			return false;
		}
	}
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FDisjoint
//
//	@doc:
//		Determine if given vector is disjoint
//
//---------------------------------------------------------------------------
BOOL
CBitVector::FDisjoint
	(
	const CBitVector *pbv
	)
	const
{
	GPOS_ASSERT(m_cBits == pbv->m_cBits && m_cUnits == pbv->m_cUnits && 
		"vectors must be of same size");

	for(ULONG i = 0; i < m_cUnits; i++)
	{
		if (0 != (m_rgull[i] & pbv->m_rgull[i]))
		{
			return false;
		}
	}
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FEqual
//
//	@doc:
//		Determine if equal
//
//---------------------------------------------------------------------------
BOOL
CBitVector::FEqual
	(
	const CBitVector *pbv
	)
	const
{
	GPOS_ASSERT(m_cBits == pbv->m_cBits && m_cUnits == pbv->m_cUnits && 
		"vectors must be of same size");
		
	// compare all components
	if (0 == clib::IMemCmp(m_rgull, pbv->m_rgull, m_cUnits * BYTES_PER_UNIT))
	{
		GPOS_ASSERT(this->FSubset(pbv) && pbv->FSubset(this));
		return true;
	}
	
	return false;
}



//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FEmpty
//
//	@doc:
//		Determine if vector is empty
//
//---------------------------------------------------------------------------
BOOL
CBitVector::FEmpty() const
{
	for (ULONG i = 0; i < m_cUnits; i++)
	{
		if (0 != m_rgull[i])
		{
			return false;
		}
	}
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::FNextBit
//
//	@doc:
//		Determine the next bit set greater or equal than the provided position
//
//---------------------------------------------------------------------------
BOOL
CBitVector::FNextBit
	(
	ULONG ulStart,
	ULONG &ulNext
	)
	const
{	
	ULONG ulOffset = ulStart % BITS_PER_UNIT;
	for (ULONG cUnit = ulStart / BITS_PER_UNIT; cUnit < m_cUnits; cUnit++)
	{
		ULLONG ull = m_rgull[cUnit] >> ulOffset;
		
		ULONG ulBit = ulOffset;
		while(0 != ull && 0 == (ull & (ULLONG)1))
		{
			ull >>= 1;
			ulBit++;
		}
		
		// if any bits left we found the next set position
		if (0 != ull)
		{
			ulNext = ulBit + (cUnit * BITS_PER_UNIT);
			return true;
		}
		
		// the initial offset applies only to the first chunk
		ulOffset = 0;
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::CElements
//
//	@doc:
//		Count bits in vector
//
//---------------------------------------------------------------------------
ULONG
CBitVector::CElements() const
{
	ULONG cBits = 0;
	for (ULONG i = 0; i < m_cUnits; i++)
	{	
		ULLONG ull = m_rgull[i];
		ULONG j = 0;
	
		for(j = 0; ull != 0; j++)
		{
			ull &= (ull - 1);
		}

		cBits += j;
	}
	
	return cBits;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitVector::UlHash
//
//	@doc:
//		Compute hash value for bit vector
//
//---------------------------------------------------------------------------
ULONG
CBitVector::UlHash() const
{
	return gpos::UlHashByteArray((BYTE*)&m_rgull[0], GPOS_SIZEOF(m_rgull[0]) * m_cUnits);
}
		
		
// EOF

