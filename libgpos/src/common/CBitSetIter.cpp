//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CBitSetIter.cpp
//
//	@doc:
//		Implementation of bitset iterator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CBitSetIter.h"
#include "gpos/common/CAutoRef.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CBitSetIter::CBitSetIter
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CBitSetIter::CBitSetIter
	(
	const CBitSet &bs
	)
	:
	m_bs(bs),
	m_ulCursor((ULONG)-1),
	m_pbsl(NULL),
	m_fActive(true)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CBitSetIter::Advance
//
//	@doc:
//		Move to next bit
//
//---------------------------------------------------------------------------
BOOL
CBitSetIter::FAdvance()
{
	GPOS_ASSERT(m_fActive && "called advance on exhausted iterator");
	
	if (NULL == m_pbsl)
	{
		m_pbsl = m_bs.m_bsllist.PtFirst();
	}
	
	while (NULL != m_pbsl)
	{
		if (m_ulCursor + 1 <= m_bs.m_cSizeBits &&
			m_pbsl->Pbv()->FNextBit(m_ulCursor + 1, m_ulCursor))
		{
			break;
		}

		m_pbsl = m_bs.m_bsllist.PtNext(m_pbsl);
		m_ulCursor = (ULONG)-1;
	}

	m_fActive = (NULL != m_pbsl);
	return m_fActive;
}
	

//---------------------------------------------------------------------------
//	@function:
//		CBitSetIter::UlBit
//
//	@doc:
//		Return current position of cursor
//
//---------------------------------------------------------------------------
ULONG
CBitSetIter::UlBit() const
{
	GPOS_ASSERT(m_fActive && NULL != m_pbsl && "iterator uninitialized");
	GPOS_ASSERT(m_pbsl->Pbv()->FBit(m_ulCursor));
	
	return m_pbsl->UlOffset() + m_ulCursor;
}

// EOF

