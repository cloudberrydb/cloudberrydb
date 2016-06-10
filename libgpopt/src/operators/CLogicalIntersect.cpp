//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalIntersect.cpp
//
//	@doc:
//		Implementation of Intersect operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CLogicalIntersect.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalIntersectAll.h"
#include "gpopt/operators/CLogicalGbAgg.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::CLogicalIntersect
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalIntersect::CLogicalIntersect
	(
	IMemoryPool *pmp
	)
	:
	CLogicalSetOp(pmp)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::CLogicalIntersect
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalIntersect::CLogicalIntersect
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrInput
	)
	:
	CLogicalSetOp(pmp, pdrgpcrOutput, pdrgpdrgpcrInput)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::~CLogicalIntersect
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalIntersect::~CLogicalIntersect()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalIntersect::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// contradictions produce no rows
	if (CDrvdPropRelational::Pdprel(exprhdl.Pdp())->Ppc()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	CMaxCard maxcardL = exprhdl.Pdprel(0)->Maxcard();
	CMaxCard maxcardR = exprhdl.Pdprel(1)->Maxcard();

	if (maxcardL <= maxcardR)
	{
		return maxcardL;
	}

	return maxcardR;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalIntersect::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOutput, phmulcr, fMustExist);
	DrgDrgPcr *pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(pmp, m_pdrgpdrgpcrInput, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalIntersect(pmp, pdrgpcrOutput, pdrgpdrgpcrInput);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalIntersect::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfIntersect2Join);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalIntersect::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalIntersect::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);

	// intersect is transformed into a group by over an intersect all
	// we follow the same route to compute statistics

	DrgPcrs *pdrgpcrsOutput = GPOS_NEW(pmp) DrgPcrs(pmp);
	const ULONG ulSize = m_pdrgpdrgpcrInput->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, (*m_pdrgpdrgpcrInput)[ul]);
		pdrgpcrsOutput->Append(pcrs);
	}

	IStatistics *pstatsIntersectAll =
			CLogicalIntersectAll::PstatsDerive(pmp, exprhdl, m_pdrgpdrgpcrInput, pdrgpcrsOutput);

	// computed columns
	DrgPul *pdrgpulComputedCols = GPOS_NEW(pmp) DrgPul(pmp);

	IStatistics *pstats = CLogicalGbAgg::PstatsDerive
											(
											pmp,
											pstatsIntersectAll,
											(*m_pdrgpdrgpcrInput)[0], // we group by the columns of the first child
											pdrgpulComputedCols, // no computed columns for set ops
											NULL // no keys, use all grouping cols
											);
	// clean up
	pdrgpulComputedCols->Release();
	pstatsIntersectAll->Release();
	pdrgpcrsOutput->Release();

	return pstats;
}

// EOF
