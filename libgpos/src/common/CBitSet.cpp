//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBitSet.cpp
//
//	@doc:
//		Implementation of bit sets
//
//		Underlying assumption: a set contains only a few links of bitvectors
//		hence, keeping them in a linked list is efficient;
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/common/CBitSet.h"
#include "gpos/common/CBitSetIter.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CBitSetLink
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink::CBitSetLink
	(
	IMemoryPool *pmp, 
	ULONG ulOffset, 
	ULONG cSizeBits
	)
	: 
	m_ulOffset(ulOffset)
{
	m_pbv = GPOS_NEW(pmp) CBitVector(pmp, cSizeBits);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetLink
//
//	@doc:
//		copy ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink::CBitSetLink
	(
	IMemoryPool *pmp, 
	const CBitSetLink &bsl
	)
	: 
	m_ulOffset(bsl.m_ulOffset)
{
	m_pbv = GPOS_NEW(pmp) CBitVector(pmp, *bsl.Pbv());
}


//---------------------------------------------------------------------------
//	@function:
//		~CBitSetLink
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink::~CBitSetLink()
{
	GPOS_DELETE(m_pbv);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::PbslLocate
//
//	@doc:
//		Find bit set link for a given offset; if non-existent return previous
//		link (may be NULL);
//		By providing a starting link we can implement a number of operations
//		in one sweep, ie O(N) 
//
//---------------------------------------------------------------------------
CBitSet::CBitSetLink *
CBitSet::PbslLocate
	(
	ULONG ulOffset,
	CBitSetLink *pbsl
	)
	const
{
	CBitSetLink *pbslFound = NULL;
	CBitSetLink *pbslCursor = pbsl;
	
	if (NULL == pbsl)
	{
		// if no cursor provided start with first element
		pbslCursor = m_bsllist.PtFirst();
	}
	else
	{
		GPOS_ASSERT(pbsl->UlOffset() <= ulOffset && "invalid start cursor");
		pbslFound = pbsl;
	}
	
	GPOS_ASSERT_IMP(NULL != pbslCursor, GPOS_OK == m_bsllist.EresFind(pbslCursor) && "cursor not in list");
	
	while(1)
	{
		// no more links or we've overshot the target
		if (NULL == pbslCursor || pbslCursor->UlOffset() > ulOffset)
		{
			break;
		}
		
		pbslFound = pbslCursor;		
		pbslCursor = m_bsllist.PtNext(pbslCursor);
	}
	
	GPOS_ASSERT_IMP(pbslFound, pbslFound->UlOffset() <= ulOffset);
	return pbslFound;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::RecomputeSize
//
//	@doc:
//		Compute size of set by adding up sizes of links
//
//---------------------------------------------------------------------------
void
CBitSet::RecomputeSize()
{
	m_cElements = 0;
	CBitSetLink *pbsl = NULL;
	
	for (
		pbsl = m_bsllist.PtFirst();
		pbsl != NULL;
		pbsl = m_bsllist.PtNext(pbsl)
		)
	{
		m_cElements += pbsl->Pbv()->CElements();
	}	
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Clear
//
//	@doc:
//		release all links
//
//---------------------------------------------------------------------------
void
CBitSet::Clear()
{
	CBitSetLink *pbsl = NULL;
	
	while(NULL != (pbsl = m_bsllist.PtFirst()))
	{		
		CBitSetLink *pbslRemove = pbsl;
		pbsl = m_bsllist.PtNext(pbsl);
		
		m_bsllist.Remove(pbslRemove);
		GPOS_DELETE(pbslRemove);
	}
	
	RecomputeSize();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::UlOffset
//
//	@doc:
//		Compute offset 
//
//---------------------------------------------------------------------------
ULONG
CBitSet::UlOffset
	(
	ULONG ul
	)
	const
{
	return (ul / m_cSizeBits) * m_cSizeBits;
}



//---------------------------------------------------------------------------
//	@function:
//		CBitSet::CBitSet
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSet::CBitSet
	(
	IMemoryPool *pmp,
	ULONG cSizeBits
	)
	:
	m_pmp(pmp),
	m_cSizeBits(cSizeBits),
	m_cElements(0)
{
	m_bsllist.Init(GPOS_OFFSET(CBitSetLink, m_link));
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::CBitSet
//
//	@doc:
//		copy ctor;
//
//---------------------------------------------------------------------------
CBitSet::CBitSet
	(
	IMemoryPool *pmp,
	const CBitSet &bs
	)
	:
	m_pmp(pmp),
	m_cSizeBits(bs.m_cSizeBits),
	m_cElements(0)
{
	m_bsllist.Init(GPOS_OFFSET(CBitSetLink, m_link));
	Union(&bs);
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::~CBitSet
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CBitSet::~CBitSet()
{
	Clear();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FBit
//
//	@doc:
//		Check if given bit is set
//
//---------------------------------------------------------------------------
BOOL 
CBitSet::FBit
	(
	ULONG ulBit
	)
	const
{
	ULONG ulOffset = UlOffset(ulBit);
	
	CBitSetLink *pbsl = PbslLocate(ulOffset);
	if (NULL != pbsl && pbsl->UlOffset() == ulOffset)
	{
		return pbsl->Pbv()->FBit(ulBit - ulOffset);
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FExchangeSet
//
//	@doc:
//		Set given bit; return previous value; allocate new link if necessary
//
//---------------------------------------------------------------------------
BOOL 
CBitSet::FExchangeSet
	(
	ULONG ulBit
	)
{
	ULONG ulOffset = UlOffset(ulBit);
	
	CBitSetLink *pbsl = PbslLocate(ulOffset);
	if (NULL == pbsl || pbsl->UlOffset() != ulOffset)
	{
		CBitSetLink *pbslNew = GPOS_NEW(m_pmp) CBitSetLink(m_pmp, ulOffset, m_cSizeBits);
		if (NULL == pbsl)
		{
			m_bsllist.Prepend(pbslNew);
		}
		else
		{
			// insert after found link
			m_bsllist.Append(pbslNew, pbsl);
		}
		
		pbsl = pbslNew;
	}
	
	GPOS_ASSERT(pbsl->UlOffset() == ulOffset);
	
	BOOL fBit = pbsl->Pbv()->FExchangeSet(ulBit - ulOffset);
	if (!fBit)
	{
		m_cElements++;
	}
	
	return fBit;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FExchangeClear
//
//	@doc:
//		Clear given bit; return previous value
//
//---------------------------------------------------------------------------
BOOL 
CBitSet::FExchangeClear
	(
	ULONG ulBit
	)
{
	ULONG ulOffset = UlOffset(ulBit);
	
	CBitSetLink *pbsl = PbslLocate(ulOffset);
	if (NULL != pbsl && pbsl->UlOffset() == ulOffset)
	{
		BOOL fBit = pbsl->Pbv()->FExchangeClear(ulBit - ulOffset);
		
		// remove empty link
		if (pbsl->Pbv()->FEmpty())
		{
			m_bsllist.Remove(pbsl);
			GPOS_DELETE(pbsl);
		}
		
		if (fBit)
		{
			m_cElements--;
		}
		
		return fBit;
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Union
//
//	@doc:
//		Union with given other set;
//		(1) determine which links need to be allocated before(!) modifying
//			the set allocate and copy missing links aside
//		(2) insert the new links into the list
//		(3) union all links, old and new, on a per-bitvector basis
//
//		For clarity step (2) and (3) are separated;
//
//---------------------------------------------------------------------------
void
CBitSet::Union
	(
	const CBitSet *pbsOther
	)
{
	CBitSetLink *pbsl = NULL;
	CBitSetLink *pbslOther = NULL;

	// dynamic array of CBitSetLink
	typedef CDynamicPtrArray <CBitSetLink, CleanupNULL> DrgBSL;

	CAutoRef<DrgBSL> a_drgpbsl;
	a_drgpbsl = GPOS_NEW(m_pmp) DrgBSL(m_pmp);
	
	// iterate through other's links and copy missing links to array
	for (
		pbslOther = pbsOther->m_bsllist.PtFirst();
		pbslOther != NULL;
		pbslOther = pbsOther->m_bsllist.PtNext(pbslOther)
		)
	{
		pbsl = PbslLocate(pbslOther->UlOffset(), pbsl);
		if (NULL == pbsl || pbsl->UlOffset() != pbslOther->UlOffset())
		{
			// need to copy this link
			CAutoP<CBitSetLink> a_pbsl;
			a_pbsl = GPOS_NEW(m_pmp) CBitSetLink(m_pmp, *pbslOther);
			a_drgpbsl->Append(a_pbsl.Pt());
			
			a_pbsl.PtReset();
		}
	}

	// insert all new links
	pbsl = NULL;
	for (ULONG i = 0; i < a_drgpbsl->UlLength(); i++)
	{
		CBitSetLink *pbslInsert = (*a_drgpbsl)[i];
		pbsl = PbslLocate(pbslInsert->UlOffset(), pbsl);
		
		GPOS_ASSERT_IMP(NULL != pbsl, pbsl->UlOffset() < pbslInsert->UlOffset());
		if (NULL == pbsl)
		{
			m_bsllist.Prepend(pbslInsert);
		}
		else
		{
			m_bsllist.Append(pbslInsert, pbsl);
		}
	}
	
	// iterate through all links and union them up
	pbslOther = NULL;
	pbsl = m_bsllist.PtFirst();
	while (NULL != pbsl)
	{
		pbslOther = pbsOther->PbslLocate(pbsl->UlOffset(), pbslOther);
		if (NULL != pbslOther && pbslOther->UlOffset() == pbsl->UlOffset())
		{
			pbsl->Pbv()->Union(pbslOther->Pbv());
		}
		
		pbsl = m_bsllist.PtNext(pbsl);
	}
	
	RecomputeSize();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Intersection
//
//	@doc:
//		Iterate through all links and intersect them; release unused links
//
//---------------------------------------------------------------------------
void
CBitSet::Intersection
	(
	const CBitSet *pbsOther
	)
{
	CBitSetLink *pbslOther = NULL;
	CBitSetLink *pbsl = m_bsllist.PtFirst();
	
	while (NULL != pbsl)
	{
		CBitSetLink *pbslRemove = NULL;

		pbslOther = pbsOther->PbslLocate(pbsl->UlOffset(), pbslOther);
		if (NULL != pbslOther && pbslOther->UlOffset() == pbsl->UlOffset())
		{
			pbsl->Pbv()->Intersection(pbslOther->Pbv());
			pbsl = m_bsllist.PtNext(pbsl);
		}
		else
		{
			pbslRemove = pbsl;
			pbsl = m_bsllist.PtNext(pbsl);
			
			m_bsllist.Remove(pbslRemove);
			GPOS_DELETE(pbslRemove);
		}
	}
	
	RecomputeSize();
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::Difference
//
//	@doc:
//		Substract other set from this by iterating through other set and
//		explicit removal of elements;
//
//---------------------------------------------------------------------------
void
CBitSet::Difference
	(
	const CBitSet *pbs
	)
{
	if (FDisjoint(pbs))
	{
		return;
	}
	
	CBitSetIter bsiter(*pbs);
	while (bsiter.FAdvance())
	{
		(void) FExchangeClear(bsiter.UlBit());
	}
}	


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FSubset
//
//	@doc:
//		Determine if given vector is subset
//
//---------------------------------------------------------------------------
BOOL
CBitSet::FSubset
	(
	const CBitSet *pbsOther
	)
	const
{
	// skip iterating if we can already tell by the sizes
	if (CElements() < pbsOther->CElements())
	{
		return false;
	}

	CBitSetLink *pbsl = NULL;
	CBitSetLink *pbslOther = NULL;

	// iterate through other's links and check for subsets
	for (
		pbslOther = pbsOther->m_bsllist.PtFirst();
		pbslOther != NULL;
		pbslOther = pbsOther->m_bsllist.PtNext(pbslOther)
		)
	{
		pbsl = PbslLocate(pbslOther->UlOffset(), pbsl);
		
		if (NULL == pbsl ||
			pbsl->UlOffset() != pbslOther->UlOffset() ||
			!pbsl->Pbv()->FSubset(pbslOther->Pbv()))
		{
			return false;
		}
	}
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FEqual
//
//	@doc:
//		Determine if equal
//
//---------------------------------------------------------------------------
BOOL
CBitSet::FEqual
	(
	const CBitSet *pbsOther
	)
	const
{
	// check pointer equality first
	if (this == pbsOther)
	{
		return true;
	}

	// skip iterating if we can already tell by the sizes
	if (CElements() != pbsOther->CElements())
	{
		return false;
	}

	CBitSetLink *pbsl = m_bsllist.PtFirst();
	CBitSetLink *pbslOther = pbsOther->m_bsllist.PtFirst();

	while(NULL != pbsl)
	{
		if (NULL == pbslOther || 
			pbsl->UlOffset() != pbslOther->UlOffset() ||
			!pbsl->Pbv()->FEqual(pbslOther->Pbv()))
		{
			return false;
		}
				
		pbsl = m_bsllist.PtNext(pbsl);
		pbslOther = pbsOther->m_bsllist.PtNext(pbslOther);
	}
	
	// same length implies pbslOther must have reached end as well
	return pbslOther == NULL;
}



//---------------------------------------------------------------------------
//	@function:
//		CBitSet::FDisjoint
//
//	@doc:
//		Determine if disjoint
//
//---------------------------------------------------------------------------
BOOL
CBitSet::FDisjoint
	(
	const CBitSet *pbsOther
	)
	const
{
	CBitSetLink *pbsl = NULL;
	CBitSetLink *pbslOther = NULL;

	// iterate through other's links an check if disjoint
	for (
		pbslOther = pbsOther->m_bsllist.PtFirst();
		pbslOther != NULL;
		pbslOther = pbsOther->m_bsllist.PtNext(pbslOther)
		)
	{
		pbsl = PbslLocate(pbslOther->UlOffset(), pbsl);
		
		if (NULL != pbsl && 
			pbsl->UlOffset() == pbslOther->UlOffset() &&
			!pbsl->Pbv()->FDisjoint(pbslOther->Pbv()))
		{
			return false;
		}
	}
	
	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::UlHash
//
//	@doc:
//		Compute hash value for set
//
//---------------------------------------------------------------------------
ULONG
CBitSet::UlHash() const
{
	ULONG ulHash = 0;

	CBitSetLink *pbsl = m_bsllist.PtFirst();
	while (NULL != pbsl)
	{
		ulHash = gpos::UlCombineHashes(ulHash, pbsl->Pbv()->UlHash());
		pbsl = m_bsllist.PtNext(pbsl);
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSet::OsPrint
//
//	@doc:
//		Debug print function
//
//---------------------------------------------------------------------------
IOstream &
CBitSet::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "{";

	ULONG ulElems = CElements();
	CBitSetIter bsiter(*this);

	for (ULONG ul = 0; ul < ulElems; ul++)
	{
		(void) bsiter.FAdvance();
		os << bsiter.UlBit();

		if (ul < ulElems - 1)
		{
			os << ", ";
		}
	}
	
	os << "} " << "Hash:" << UlHash();
	
	return os;
}


// EOF
