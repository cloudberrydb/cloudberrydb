//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalApply.cpp
//
//	@doc:
//		Implementation of apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalApply.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::CLogicalApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalApply::CLogicalApply
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pdrgpcrInner(NULL),
	m_eopidOriginSubq(COperator::EopSentinel)
{}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::CLogicalApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalApply::CLogicalApply
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrInner,
	EOperatorId eopidOriginSubq
	)
	:
	CLogical(pmp),
	m_pdrgpcrInner(pdrgpcrInner),
	m_eopidOriginSubq(eopidOriginSubq)
{
	GPOS_ASSERT(NULL != pdrgpcrInner);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::~CLogicalApply
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalApply::~CLogicalApply()
{
	CRefCount::SafeRelease(m_pdrgpcrInner);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::PcrsStat
//
//	@doc:
//		Compute required stat columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalApply::PcrsStat
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	ULONG ulChildIndex
	)
	const
{
	GPOS_ASSERT(3 == exprhdl.UlArity());

	CColRefSet *pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
	// add columns used by scalar child
	pcrsUsed->Union(exprhdl.Pdpscalar(2)->PcrsUsed());

	if (0 == ulChildIndex)
	{
		// add outer references coming from inner child
		pcrsUsed->Union(exprhdl.Pdprel(1)->PcrsOuter());
	}

	CColRefSet *pcrsStat = PcrsReqdChildStats(pmp, exprhdl, pcrsInput, pcrsUsed, ulChildIndex);
	pcrsUsed->Release();

	return pcrsStat;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalApply::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		DrgPcr *pdrgpcrInner = CLogicalApply::PopConvert(pop)->PdrgPcrInner();
		if (NULL == m_pdrgpcrInner || NULL == pdrgpcrInner)
		{
			return 	 (NULL == m_pdrgpcrInner && NULL == pdrgpcrInner);
		}

		return m_pdrgpcrInner->FEqual(pdrgpcrInner);
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalApply::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalApply::OsPrint
	(
	IOstream &os
	)
	const
{
	os << this->SzId();
	if (NULL != m_pdrgpcrInner)
	{
		os << " (Reqd Inner Cols: ";
		(void) CUtils::OsPrintDrgPcr(os, m_pdrgpcrInner);
		os << ")";
	}

	return os;
}


// EOF

