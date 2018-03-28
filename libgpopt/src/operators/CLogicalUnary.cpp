//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalUnary.cpp
//
//	@doc:
//		Implementation of logical unary operators
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalUnary.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/statistics/CProjectStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalUnary::FMatch
	(
	COperator *pop
	)
	const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnary::Esp
//
//	@doc:
//		Promise level for stat derivation
//
//---------------------------------------------------------------------------
CLogical::EStatPromise
CLogicalUnary::Esp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	// low promise for stat derivation if scalar predicate has subqueries, or logical
	// expression has outer-refs or is part of an Apply expression
	if (exprhdl.Pdpscalar(1)->FHasSubquery() ||
		exprhdl.FHasOuterRefs() ||
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
//		CLogicalUnary::PstatsDeriveProject
//
//	@doc:
//		Derive statistics for projection operators
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnary::PstatsDeriveProject
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	HMUlDatum *phmuldatum
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *pstatsChild = exprhdl.Pstats(0);
	CReqdPropRelational *prprel = CReqdPropRelational::Prprel(exprhdl.Prp());
	CColRefSet *pcrs = prprel->PcrsStat();
	DrgPul *pdrgpulColIds = GPOS_NEW(pmp) DrgPul(pmp);
	pcrs->ExtractColIds(pmp, pdrgpulColIds);

	IStatistics *pstats = CProjectStatsProcessor::PstatsProject(pmp, dynamic_cast<CStatistics *>(pstatsChild), pdrgpulColIds, phmuldatum);

	// clean up
	pdrgpulColIds->Release();

	return pstats;
}

// EOF
