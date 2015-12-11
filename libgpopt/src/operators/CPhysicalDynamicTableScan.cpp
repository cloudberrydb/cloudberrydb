//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalDynamicTableScan.cpp
//
//	@doc:
//		Implementation of dynamic table scan operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CPartIndexMap.h"

#include "gpopt/operators/CPhysicalDynamicTableScan.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicTableScan::CPhysicalDynamicTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalDynamicTableScan::CPhysicalDynamicTableScan
	(
	IMemoryPool *pmp,
	BOOL fPartial,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId,
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
//		CPhysicalDynamicTableScan::FMatch
//
//	@doc:
//		match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDynamicTableScan::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchDynamicScan(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicTableScan::PstatsDerive
//
//	@doc:
//		Statistics derivation during costing
//
//---------------------------------------------------------------------------
IStatistics *
CPhysicalDynamicTableScan::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *prpplan,
	DrgPstat * // pdrgpstatCtxt
	)
	const
{
	GPOS_ASSERT(NULL != prpplan);

	return CStatisticsUtils::PstatsDynamicScan(pmp, exprhdl, UlScanId(), prpplan->Pepp()->PpfmDerived());
}

// EOF
