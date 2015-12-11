//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc..
//
//	@filename:
//		CLogicalMaxOneRow.cpp
//
//	@doc:
//		Implementation of logical MaxOneRow operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalMaxOneRow.h"

#include "gpopt/xforms/CXformUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalMaxOneRow::Esp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// low promise for stat derivation if logical expression has outer-refs
	// or is part of an Apply expression
	if (exprhdl.FHasOuterRefs() ||
		 (NULL != exprhdl.Pgexpr() &&
			CXformUtils::FGenerateApply(exprhdl.Pgexpr()->ExfidOrigin()))
		)
	{
		 return EspLow;
	}

	return EspHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PcrsStat
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalMaxOneRow::PcrsStat
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	ULONG ulChildIndex
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Union(pcrsInput);

	// intersect with the output columns of relational child
	pcrs->Intersection(exprhdl.Pdprel(ulChildIndex)->PcrsOutput());

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PxfsCandidates
//
//	@doc:
//		Compute candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalMaxOneRow::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfMaxOneRow2Assert);
	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalMaxOneRow::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalMaxOneRow::PstatsDerive
			(
			IMemoryPool *pmp,
			CExpressionHandle &exprhdl,
			DrgPstat * // pdrgpstatCtxt
			)
			const
{
	// no more than one row can be produced by operator, scale down input statistics accordingly
	IStatistics *pstats = exprhdl.Pstats(0);
	return  pstats->PstatsScale(pmp, CDouble(1.0 / pstats->DRows()));
}


// EOF

