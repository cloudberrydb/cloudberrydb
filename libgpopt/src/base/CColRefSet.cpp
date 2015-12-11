//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColRefSet.cpp
//
//	@doc:
//		Implementation of column reference set based on bit sets
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/base/CColRefSetIter.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColumnFactory.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::CColRefSet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CColRefSet::CColRefSet
	(
	IMemoryPool *pmp,
	ULONG ulSizeBits
	)
	:
	CBitSet(pmp, ulSizeBits)
{}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::CColRefSet
//
//	@doc:
//		copy ctor;
//
//---------------------------------------------------------------------------
CColRefSet::CColRefSet
	(
	IMemoryPool *pmp,
	const CColRefSet &bs
	)
	:
	CBitSet(pmp, bs)
{}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::CColRefSet
//
//	@doc:
//		ctor, copy from col refs array
//
//---------------------------------------------------------------------------
CColRefSet::CColRefSet
	(
	IMemoryPool *pmp,
	const DrgPcr *pdrgpcr,
	ULONG ulSize
	)
	:
	CBitSet(pmp, ulSize)
{
	Include(pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::~CColRefSet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CColRefSet::~CColRefSet()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::FMember
//
//	@doc:
//		Check if given column ref is in the set
//
//---------------------------------------------------------------------------
BOOL 
CColRefSet::FMember
	(
	const CColRef *pcr
	)
	const
{
	return CBitSet::FBit(pcr->UlId());
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::PcrAny
//
//	@doc:
//		Return random member
//
//---------------------------------------------------------------------------
CColRef *
CColRefSet::PcrAny() const
{
	// for now return the first column
	return PcrFirst();
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::PcrFirst
//
//	@doc:
//		Return first member
//
//---------------------------------------------------------------------------
CColRef *
CColRefSet::PcrFirst() const
{
	CColRefSetIter crsi(*this);
	if (crsi.FAdvance())
	{
		return crsi.Pcr();
	}
	
	GPOS_ASSERT(0 == CElements());
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Include
//
//	@doc:
//		Include a constant column ref in set
//
//---------------------------------------------------------------------------
void
CColRefSet::Include
	(
	const CColRef *pcr
	)
{
	CBitSet::FExchangeSet(pcr->UlId());
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Include
//
//	@doc:
//		Include column refs from an array
//
//---------------------------------------------------------------------------
void 
CColRefSet::Include
	(
	const DrgPcr *pdrgpcr
	)
{
	ULONG ulLength = pdrgpcr->UlLength();
	for (ULONG i = 0; i < ulLength; i++)
	{
		Include((*pdrgpcr)[i]);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Include
//
//	@doc:
//		Include a set of columns in bitset
//
//---------------------------------------------------------------------------
void
CColRefSet::Include
	(
	const CColRefSet *pcrs
	)
{
	CColRefSetIter crsi(*pcrs);
	while(crsi.FAdvance())
	{
		Include(crsi.Pcr());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Exclude
//
//	@doc:
//		Remove column from bitset
//
//---------------------------------------------------------------------------
void 
CColRefSet::Exclude
	(
	const CColRef *pcr
	)
{
	CBitSet::FExchangeClear(pcr->UlId());
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Exclude
//
//	@doc:
//		Remove a set of columns from bitset
//
//---------------------------------------------------------------------------
void
CColRefSet::Exclude
	(
	const CColRefSet *pcrs
	)
{
	CColRefSetIter crsi(*pcrs);
	while(crsi.FAdvance())
	{
		Exclude(crsi.Pcr());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Exclude
//
//	@doc:
//		Remove an array of columns from bitset
//
//---------------------------------------------------------------------------
void
CColRefSet::Exclude
	(
	const DrgPcr *pdrgpcr
	)
{
	for (ULONG i = 0; i < pdrgpcr->UlLength(); i++)
	{
		Exclude((*pdrgpcr)[i]);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Replace
//
//	@doc:
//		Replace column with another column in bitset
//
//---------------------------------------------------------------------------
void
CColRefSet::Replace
	(
	const CColRef *pcrOut,
	const CColRef *pcrIn
	)
{
	if (FMember(pcrOut))
	{
		Exclude(pcrOut);
		Include(pcrIn);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Replace
//
//	@doc:
//		Replace an array of columns with another array of columns
//
//---------------------------------------------------------------------------
void
CColRefSet::Replace
	(
	const DrgPcr *pdrgpcrOut,
	const DrgPcr *pdrgpcrIn
	)
{
	const ULONG ulLen = pdrgpcrOut->UlLength();
	GPOS_ASSERT(ulLen == pdrgpcrIn->UlLength());

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		Replace((*pdrgpcrOut)[ul], (*pdrgpcrIn)[ul]);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::Pdrgpcr
//
//	@doc:
//		Convert set into array
//
//---------------------------------------------------------------------------
DrgPcr *
CColRefSet::Pdrgpcr
	(
	IMemoryPool *pmp
	)
	const
{
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	
	CColRefSetIter crsi(*this);
	while(crsi.FAdvance())
	{
		pdrgpcr->Append(crsi.Pcr());
	}
	
	return pdrgpcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::UlHash
//
//	@doc:
//		Compute hash value by combining hashes of components
//
//---------------------------------------------------------------------------
ULONG
CColRefSet::UlHash()
{
	ULONG ulSize = this->CElements();
	ULONG ulHash = gpos::UlHash<ULONG>(&ulSize);
	
	// limit the number of columns used in hash computation
	ULONG ulLen = std::min(ulSize, (ULONG) 8);

	CColRefSetIter crsi(*this);
	for (ULONG i = 0; i < ulLen; i++)
	{
		(void) crsi.FAdvance();
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(crsi.Pcr()));
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::OsPrint
//
//	@doc:
//		Helper function to print a colref set
//
//---------------------------------------------------------------------------
IOstream &
CColRefSet::OsPrint
	(
	IOstream &os,
	ULONG ulLenMax
	)
	const
{
	ULONG ulLen = CElements();
	ULONG ul = 0;
	
	CColRefSetIter crsi(*this);
	while(crsi.FAdvance() && ul < std::min(ulLen, ulLenMax))
	{
		CColRef *pcr = crsi.Pcr();
		pcr->OsPrint(os);
		if (ul < ulLen - 1)
		{
			os << ", ";
		}
		ul++;
	}
	
	if (ulLenMax < ulLen)
	{
		os << "...";
	}

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::ExtractColIds
//
//	@doc:
//		Extract array of column ids from colrefset
//
//---------------------------------------------------------------------------
void
CColRefSet::ExtractColIds
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulColIds
	)
	const
{
	CColRefSetIter crsi(*this);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		ULONG ulColId = pcr->UlId();
		pdrgpulColIds->Append(GPOS_NEW(pmp) ULONG(ulColId));
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::FContained
//
//	@doc:
//		Check if the current colrefset is a subset of any of the colrefsets in
//		the given array
//
//---------------------------------------------------------------------------
BOOL
CColRefSet::FContained
	(
	const DrgPcrs *pdrgpcrs
	)
{
	GPOS_ASSERT(NULL != pdrgpcrs);

	const ULONG ulLen = pdrgpcrs->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		if ((*pdrgpcrs)[ul]->FSubset(this))
		{
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CColRefSet::FCovered
//
//	@doc:
//		Are the columns in the column reference set covered by the array of
//		column ref sets
//---------------------------------------------------------------------------
BOOL
CColRefSet::FCovered
	(
	DrgPcrs *pdrgpcrs,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pdrgpcrs);
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(0 < pdrgpcrs->UlLength());

	if (0 == pcrs->CElements())
	{
		return false;
	}

	CColRefSetIter crsi(*pcrs);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		BOOL fFound = false;
		const ULONG ulLen =  pdrgpcrs->UlLength();
		for (ULONG ul = 0; ul < ulLen && !fFound; ul++)
		{
			CColRefSet *pcrs = (*pdrgpcrs)[ul];
			if (pcrs->FMember(pcr))
			{
				fFound = true;
			}
		}

		if (!fFound)
		{
			return false;
		}
	}

	return true;
}


// EOF
