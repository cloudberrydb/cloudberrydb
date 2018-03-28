//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CPhysicalDynamicBitmapTableScan.cpp
//
//	@doc:
//		Dynamic bitmap table scan physical operator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalDynamicBitmapTableScan.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CFilterStatsProcessor.h"
using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicBitmapTableScan::CPhysicalDynamicBitmapTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalDynamicBitmapTableScan::CPhysicalDynamicBitmapTableScan
	(
		IMemoryPool *pmp,
		BOOL fPartial,
		CTableDescriptor *ptabdesc,
		ULONG ulOriginOpId,
		const CName *pnameAlias,
		ULONG ulScanId,
		DrgPcr *pdrgpcrOutput,
		DrgDrgPcr *pdrgpdrgpcrParts,
		ULONG ulSecondaryScanId,
		CPartConstraint *ppartcnstr,
		CPartConstraint *ppartcnstrRel
	)
	:
	CPhysicalDynamicScan(pmp, fPartial, ptabdesc, ulOriginOpId, pnameAlias, ulScanId, pdrgpcrOutput, pdrgpdrgpcrParts, ulSecondaryScanId, ppartcnstr, ppartcnstrRel)
{}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicBitmapTableScan::FMatch
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDynamicBitmapTableScan::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchDynamicBitmapScan(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicBitmapTableScan::PstatsDerive
//
//	@doc:
//		Statistics derivation during costing
//
//---------------------------------------------------------------------------
IStatistics *
CPhysicalDynamicBitmapTableScan::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *prpplan,
	DrgPstat *pdrgpstatCtxt
	)
	const
{
	GPOS_ASSERT(NULL != prpplan);

	IStatistics *pstatsBaseTable = CStatisticsUtils::PstatsDynamicScan
									(
									pmp,
									exprhdl,
									UlScanId(),
									prpplan->Pepp()->PpfmDerived()
									);

	CExpression *pexprCondChild = exprhdl.PexprScalarChild(0 /*ulChidIndex*/);
	CExpression *pexprLocal = NULL;
	CExpression *pexprOuterRefs = NULL;

	// get outer references from expression handle
	CColRefSet *pcrsOuter = exprhdl.Pdprel()->PcrsOuter();

	CPredicateUtils::SeparateOuterRefs(pmp, pexprCondChild, pcrsOuter, &pexprLocal, &pexprOuterRefs);

	IStatistics *pstats = CFilterStatsProcessor::PstatsFilterForScalarExpr
							(
							pmp,
							exprhdl,
							pstatsBaseTable,
							pexprLocal,
							pexprOuterRefs,
							pdrgpstatCtxt
							);

	pstatsBaseTable->Release();
	pexprLocal->Release();
	pexprOuterRefs->Release();

	return pstats;
}

// EOF
