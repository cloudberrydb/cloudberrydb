//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelGPDB.cpp
//
//	@doc:
//		Implementation of GPDB cost model
//---------------------------------------------------------------------------

#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"
#include "gpopt/operators/CPhysicalIndexScan.h"
#include "gpopt/operators/CPhysicalDynamicIndexScan.h"
#include "gpopt/operators/CPhysicalHashAgg.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CPhysicalMotion.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "gpopt/operators/CExpression.h"
#include "gpdbcost/CCostModelGPDB.h"

using namespace gpos;
using namespace gpdbcost;


// initialization of cost functions
const CCostModelGPDB::SCostMapping CCostModelGPDB::m_rgcm[] =
{
	{COperator::EopPhysicalTableScan, CostScan},
	{COperator::EopPhysicalDynamicTableScan, CostScan},
	{COperator::EopPhysicalExternalScan, CostScan},

	{COperator::EopPhysicalFilter, CostFilter},

	{COperator::EopPhysicalIndexScan, CostIndexScan},
	{COperator::EopPhysicalDynamicIndexScan, CostIndexScan},
	{COperator::EopPhysicalBitmapTableScan, CostBitmapTableScan},
	{COperator::EopPhysicalDynamicBitmapTableScan, CostBitmapTableScan},

	{COperator::EopPhysicalSequenceProject, CostSequenceProject},

	{COperator::EopPhysicalCTEProducer, CostCTEProducer},
	{COperator::EopPhysicalCTEConsumer, CostCTEConsumer},
	{COperator::EopPhysicalConstTableGet, CostConstTableGet},
	{COperator::EopPhysicalDML, CostDML},

	{COperator::EopPhysicalHashAgg, CostHashAgg},
	{COperator::EopPhysicalHashAggDeduplicate, CostHashAgg},
	{COperator::EopPhysicalScalarAgg, CostScalarAgg},
	{COperator::EopPhysicalStreamAgg, CostStreamAgg},
	{COperator::EopPhysicalStreamAggDeduplicate, CostStreamAgg},

	{COperator::EopPhysicalSequence, CostSequence},

	{COperator::EopPhysicalSort, CostSort},

	{COperator::EopPhysicalTVF, CostTVF},

	{COperator::EopPhysicalSerialUnionAll, CostUnionAll},
	{COperator::EopPhysicalParallelUnionAll, CostUnionAll},

	{COperator::EopPhysicalInnerHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftSemiHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftAntiSemiHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftAntiSemiHashJoinNotIn, CostHashJoin},
	{COperator::EopPhysicalLeftOuterHashJoin, CostHashJoin},

	{COperator::EopPhysicalInnerIndexNLJoin, CostInnerIndexNLJoin},

	{COperator::EopPhysicalMotionGather, CostMotion},
	{COperator::EopPhysicalMotionBroadcast, CostMotion},
	{COperator::EopPhysicalMotionHashDistribute, CostMotion},
	{COperator::EopPhysicalMotionRandom, CostMotion},
	{COperator::EopPhysicalMotionRoutedDistribute, CostMotion},

	{COperator::EopPhysicalInnerNLJoin, CostNLJoin},
	{COperator::EopPhysicalLeftSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalLeftAntiSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalLeftAntiSemiNLJoinNotIn, CostNLJoin},
	{COperator::EopPhysicalLeftOuterNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedInnerNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedLeftOuterNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedLeftSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedInLeftSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedLeftAntiSemiNLJoin, CostNLJoin},
	{COperator::EopPhysicalCorrelatedNotInLeftAntiSemiNLJoin, CostNLJoin}
};

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CCostModelGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostModelGPDB::CCostModelGPDB
	(
	IMemoryPool *pmp,
	ULONG ulSegments,
	DrgPcp *pdrgpcp
	)
	:
	m_pmp(pmp),
	m_ulSegments(ulSegments)
{
	GPOS_ASSERT(0 < ulSegments);

	m_pcp = GPOS_NEW(pmp) CCostModelParamsGPDB(pmp);
	SetParams(pdrgpcp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::DRowsPerHost
//
//	@doc:
//		Return number of rows per host
//
//---------------------------------------------------------------------------
CDouble
CCostModelGPDB::DRowsPerHost
	(
	CDouble dRowsTotal
	)
	const
{
	return dRowsTotal / m_ulSegments;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::~CCostModelGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostModelGPDB::~CCostModelGPDB()
{
	m_pcp->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostTupleProcessing
//
//	@doc:
//		Cost of tuple processing
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostTupleProcessing
	(
	DOUBLE dRows,
	DOUBLE dWidth,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pcp);

	const CDouble dTupDefaultProcCostUnit = pcp->PcpLookup(CCostModelParamsGPDB::EcpTupDefaultProcCostUnit)->DVal();
	GPOS_ASSERT(0 < dTupDefaultProcCostUnit);

	return CCost(dRows * dWidth * dTupDefaultProcCostUnit);
}


//
//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostScanOutput
//
//	@doc:
//		Helper function to return cost of producing output tuples from
//		Scan operator
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostScanOutput
	(
	IMemoryPool *, // pmp
	DOUBLE dRows,
	DOUBLE dWidth,
	DOUBLE dRebinds,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pcp);

	const CDouble dOutputTupCostUnit = pcp->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)->DVal();
	GPOS_ASSERT(0 < dOutputTupCostUnit);

	return CCost(dRebinds * (dRows * dWidth * dOutputTupCostUnit));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostUnary
//
//	@doc:
//		Helper function to return cost of a plan rooted by unary operator
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostUnary
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const SCostingInfo *pci,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != pcp);

	DOUBLE dRows = pci->DRows();
	DOUBLE dWidth = pci->DWidth();
	DOUBLE dRebinds = pci->DRebinds();

	CCost costLocal = CCost(dRebinds * CostTupleProcessing(dRows, dWidth, pcp).DVal());
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcp);

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSpooling
//
//	@doc:
//		Helper function to compute spooling cost
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSpooling
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const SCostingInfo *pci,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != pcp);

	const CDouble dMaterializeCostUnit = pcp->PcpLookup(CCostModelParamsGPDB::EcpMaterializeCostUnit)->DVal();
	GPOS_ASSERT(0 < dMaterializeCostUnit);

	DOUBLE dRows = pci->DRows();
	DOUBLE dWidth = pci->DWidth();
	DOUBLE dRebinds = pci->DRebinds();

	// materialization cost is correlated with the number of rows and width of returning tuples.
	CCost costLocal = CCost(dRebinds * (dWidth * dRows * dMaterializeCostUnit));
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcp);

	return costLocal + costChild;
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::FUnary
//
//	@doc:
//		Check if given operator is unary
//
//---------------------------------------------------------------------------
BOOL
CCostModelGPDB::FUnary
	(
	COperator::EOperatorId eopid
	)
{
	return COperator::EopPhysicalAssert == eopid ||
			 COperator::EopPhysicalComputeScalar == eopid ||
			 COperator::EopPhysicalLimit == eopid ||
			 COperator::EopPhysicalPartitionSelector == eopid ||
			 COperator::EopPhysicalPartitionSelectorDML == eopid ||
			 COperator::EopPhysicalRowTrigger == eopid ||
			 COperator::EopPhysicalSplit == eopid ||
			 COperator::EopPhysicalSpool == eopid;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostChildren
//
//	@doc:
//		Add up children costs
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostChildren
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const SCostingInfo *pci,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != pcp);

	DOUBLE *pdCost = pci->PdCost();
	const ULONG ulSize = pci->UlChildren();
	BOOL fFilterParent = (COperator::EopPhysicalFilter == exprhdl.Pop()->Eopid());

	DOUBLE dRes = 0.0;
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		DOUBLE dCostChild = pdCost[ul];
		COperator *popChild = exprhdl.Pop(ul);
		if (NULL != popChild && CUtils::FPhysicalScan(popChild))
		{
			// by default, compute scan output cost based on full Scan
			DOUBLE dScanRows = pci->PdRows()[ul];

			if (fFilterParent)
			{
				// if parent is filter, compute scan output cost based on rows produced by Filter operator
				dScanRows = pci->DRows();
			}

			dCostChild = dCostChild + CostScanOutput(pmp, dScanRows, pci->PdWidth()[ul], pci->PdRebinds()[ul], pcp).DVal();
		}

		dRes = dRes + dCostChild;
	}

	return CCost(dRes);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostMaxChild
//
//	@doc:
//		Returns cost of highest costed child
//
//---------------------------------------------------------------------------

CCost
CCostModelGPDB::CostMaxChild
(
	IMemoryPool *,
	CExpressionHandle &,
	const SCostingInfo *pci,
	ICostModelParams *
	)
{
	GPOS_ASSERT(NULL != pci);

	DOUBLE *pdCost = pci->PdCost();
	const ULONG ulSize = pci->UlChildren();

	DOUBLE dRes = 0.0;
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		if (pdCost[ul] > dRes)
		{
			dRes = pdCost[ul];
		}
	}

	return CCost(dRes);
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostCTEProducer
//
//	@doc:
//		Cost of CTE producer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostCTEProducer
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEProducer == exprhdl.Pop()->Eopid());

	CCost cost = CostUnary(pmp, exprhdl, pci, pcmgpdb->Pcp());

	// In GPDB, the child of a ShareInputScan representing the producer can
	// only be a materialize or sort. Here, we check if a materialize node
	// needs to be added during DXL->PlStmt translation

	COperator *popChild = exprhdl.Pop(0 /*ulChildIndex*/);
	if (NULL == popChild)
	{
		// child operator is not known, this could happen when computing cost bound
		return cost;
	}

	COperator::EOperatorId eopid = popChild->Eopid();
	if (COperator::EopPhysicalSpool != eopid && COperator::EopPhysicalSort != eopid)
	{
		// no materialize needed
		return cost;
	}

	// a materialize (spool) node is added during DXL->PlStmt translation,
	// we need to add the cost of writing the tuples to disk
	const CDouble dMaterializeCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpMaterializeCostUnit)->DVal();
	GPOS_ASSERT(0 < dMaterializeCostUnit);

	CCost costSpooling = CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth() * dMaterializeCostUnit));

	return cost + costSpooling;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostCTEConsumer
//
//	@doc:
//		Cost of CTE consumer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostCTEConsumer
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEConsumer == exprhdl.Pop()->Eopid());

	const CDouble dInitScan = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)->DVal();
	const CDouble dTableScanCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTableScanCostUnit)->DVal();
	const CDouble dOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)->DVal();
	GPOS_ASSERT(0 < dOutputTupCostUnit);
	GPOS_ASSERT(0 < dTableScanCostUnit);

	return CCost(pci->DRebinds() * (dInitScan + pci->DRows() * pci->DWidth() * (dTableScanCostUnit + dOutputTupCostUnit)));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostConstTableGet
//
//	@doc:
//		Cost of const table get
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostConstTableGet
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalConstTableGet == exprhdl.Pop()->Eopid());

	return CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostDML
//
//	@doc:
//		Cost of DML
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostDML
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalDML == exprhdl.Pop()->Eopid());

	const CDouble dTupUpdateBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTupUpdateBandwith)->DVal();
	GPOS_ASSERT(0 < dTupUpdateBandwidth);

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];

	CCost costLocal = CCost(pci->DRebinds() * (dRowsOuter * dWidthOuter) / dTupUpdateBandwidth);
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostScalarAgg
//
//	@doc:
//		Cost of scalar agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostScalarAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalScalarAgg == exprhdl.Pop()->Eopid());

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];

	// get the number of aggregate columns
	const ULONG ulAggCols = exprhdl.Pdpscalar(1)->PcrsUsed()->CElements();
	// get the number of aggregate functions
	const ULONG ulAggFunctions = exprhdl.PexprScalarChild(1)->UlArity();

	const CDouble dHashAggInputTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHashAggInputTupWidthCostUnit)->DVal();
	GPOS_ASSERT(0 < dHashAggInputTupWidthCostUnit);

	// scalarAgg cost is correlated with rows and width of the input tuples and the number of columns used in aggregate
	// It also depends on the complexity of the aggregate algorithm, which is hard to model yet shared by all the aggregate
	// operators, thus we ignore this part of cost for all.
	CCost costLocal = CCost(pci->DRebinds() * (dRowsOuter * dWidthOuter * ulAggCols * ulAggFunctions * dHashAggInputTupWidthCostUnit));

	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());
	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostStreamAgg
//
//	@doc:
//		Cost of stream agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostStreamAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

#ifdef GPOS_DEBUG
	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalStreamAgg == eopid ||
			COperator::EopPhysicalStreamAggDeduplicate == eopid);
#endif // GPOS_DEBUG

	const CDouble dHashAggOutputTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(
			CCostModelParamsGPDB::EcpHashAggOutputTupWidthCostUnit)->DVal();
	const CDouble dTupDefaultProcCostUnit = pcmgpdb->Pcp()->PcpLookup(
			CCostModelParamsGPDB::EcpTupDefaultProcCostUnit)->DVal();
	GPOS_ASSERT(0 < dHashAggOutputTupWidthCostUnit);
	GPOS_ASSERT(0 < dTupDefaultProcCostUnit);

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	// streamAgg cost is correlated with rows and width of input tuples and rows and width of output tuples
	CCost costLocal = CCost(pci->DRebinds() * (
			dRowsOuter * dWidthOuter * dTupDefaultProcCostUnit +
			pci->DRows() * pci->DWidth() * dHashAggOutputTupWidthCostUnit));
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());
	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSequence
//
//	@doc:
//		Cost of sequence
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSequence
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequence == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSort
//
//	@doc:
//		Cost of sort
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSort
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSort == exprhdl.Pop()->Eopid());

	// log operation below
	const CDouble dRows = CDouble(std::max(1.0, pci->DRows()));
	const CDouble dRebinds = CDouble(pci->DRebinds());
	const CDouble dWidth = CDouble(pci->DWidth());

	const CDouble dSortTupWidthCost = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpSortTupWidthCostUnit)->DVal();
	GPOS_ASSERT(0 < dSortTupWidthCost);

	// sort cost is correlated with the number of rows and width of input tuples. We use n*log(n) for sorting complexity.
	CCost costLocal = CCost(dRebinds * (dRows * dRows.FpLog2() * dWidth * dSortTupWidthCost));
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostTVF
//
//	@doc:
//		Cost of table valued function
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostTVF
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalTVF == exprhdl.Pop()->Eopid());

	return CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostUnionAll
//
//	@doc:
//		Cost of UnionAll
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostUnionAll
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(NULL != CPhysicalUnionAll::PopConvert(exprhdl.Pop()));

	if(COperator::EopPhysicalParallelUnionAll == exprhdl.Pop()->Eopid())
	{
		return CostMaxChild(pmp, exprhdl, pci, pcmgpdb->Pcp());
	}

	CCost costLocal = CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostHashAgg
//
//	@doc:
//		Cost of hash agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostHashAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

#ifdef GPOS_DEBUG
	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalHashAgg == eopid ||
			COperator::EopPhysicalHashAggDeduplicate == eopid);
#endif // GPOS_DEBUG

	DOUBLE dRowsOuter = pci->PdRows()[0];

	// A local hash agg may stream partial aggregates to global agg when it's hash table is full to avoid spilling.
	// This is dertermined by the order of tuples received by local agg. In the worst case, the local hash agg may
	// see a tuple from each different group until its hash table fills up all available memory, and hence it produces
	// tuples as many as its input size. On the other hand, in the best case, the local agg may receive tuples sorted
	// by grouping columns, which allows it to complete all local aggregation in memory and produce exactly tuples as
	// the number of groups.
	//
	// Since we do not know the order of tuples received by local hash agg, we assume the number of output rows from local
	// agg is the average between input and output rows

	// the cardinality out as (dRows + dRowsOuter)/2 to increase the local hash agg cost
	DOUBLE dRows = pci->DRows();
	CPhysicalHashAgg *popAgg = CPhysicalHashAgg::PopConvert(exprhdl.Pop());
	if ((COperator::EgbaggtypeLocal == popAgg->Egbaggtype()) && popAgg->FGeneratesDuplicates())
	{
		dRows = (dRows + dRowsOuter) / 2.0;
	}

	// get the number of grouping columns
	const ULONG ulGrpCols = CPhysicalHashAgg::PopConvert(exprhdl.Pop())->PdrgpcrGroupingCols()->UlLength();

	const CDouble dHashAggInputTupColumnCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHashAggInputTupColumnCostUnit)->DVal();
	const CDouble dHashAggInputTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHashAggInputTupWidthCostUnit)->DVal();
	const CDouble dHashAggOutputTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHashAggOutputTupWidthCostUnit)->DVal();
	GPOS_ASSERT(0 < dHashAggInputTupColumnCostUnit);
	GPOS_ASSERT(0 < dHashAggInputTupWidthCostUnit);
	GPOS_ASSERT(0 < dHashAggOutputTupWidthCostUnit);

	// hashAgg cost contains three parts: build hash table, aggregate tuples, and output tuples.
	// 1. build hash table is correlated with the number of rows and width of input tuples and the number of columns used.
	// 2. cost of aggregate tuples depends on the complexity of aggregation algorithm and thus is ignored.
	// 3. cost of output tuples is correlated with rows and width of returning tuples.
	CCost costLocal = CCost(pci->DRebinds() * (
		dRowsOuter * ulGrpCols * dHashAggInputTupColumnCostUnit
		+
		dRowsOuter * ulGrpCols * pci->DWidth() * dHashAggInputTupWidthCostUnit
		+
		dRows * pci->DWidth() * dHashAggOutputTupWidthCostUnit
	));
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostHashJoin
//
//	@doc:
//		Cost of hash join
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostHashJoin
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
#ifdef GPOS_DEBUG
	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	GPOS_ASSERT	(COperator::EopPhysicalInnerHashJoin == eopid ||
			COperator::EopPhysicalLeftSemiHashJoin == eopid ||
			COperator::EopPhysicalLeftAntiSemiHashJoin == eopid ||
			COperator::EopPhysicalLeftAntiSemiHashJoinNotIn == eopid ||
			COperator::EopPhysicalLeftOuterHashJoin == eopid);
#endif // GPOS_DEBUG

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];
	const DOUBLE dRowsInner = pci->PdRows()[1];
	const DOUBLE dWidthInner = pci->PdWidth()[1];

	const CDouble dHJHashTableInitCostFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJHashTableInitCostFactor)->DVal();
	const CDouble dHJHashTableColumnCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJHashTableColumnCostUnit)->DVal();
	const CDouble dHJHashTableWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJHashTableWidthCostUnit)->DVal();
	const CDouble dJoinFeedingTupColumnCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)->DVal();
	const CDouble dJoinFeedingTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)->DVal();
	const CDouble dHJHashingTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJHashingTupWidthCostUnit)->DVal();
	const CDouble dJoinOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)->DVal();
	const CDouble dHJSpillingMemThreshold = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJSpillingMemThreshold)->DVal();
	const CDouble dHJFeedingTupColumnSpillingCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJFeedingTupColumnSpillingCostUnit)->DVal();
	const CDouble dHJFeedingTupWidthSpillingCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJFeedingTupWidthSpillingCostUnit)->DVal();
	const CDouble dHJHashingTupWidthSpillingCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpHJHashingTupWidthSpillingCostUnit)->DVal();
	GPOS_ASSERT(0 < dHJHashTableInitCostFactor);
	GPOS_ASSERT(0 < dHJHashTableColumnCostUnit);
	GPOS_ASSERT(0 < dHJHashTableWidthCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dHJHashingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);
	GPOS_ASSERT(0 < dHJSpillingMemThreshold);
	GPOS_ASSERT(0 < dHJFeedingTupColumnSpillingCostUnit);
	GPOS_ASSERT(0 < dHJFeedingTupWidthSpillingCostUnit);
	GPOS_ASSERT(0 < dHJHashingTupWidthSpillingCostUnit);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond= exprhdl.PexprScalarChild(2);
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprJoinCond->PdpDerive())->PcrsUsed();
	const ULONG ulColsUsed = pcrsUsed->CElements();

	// TODO 2014-03-14
	// currently, we hard coded a spilling memory threshold for judging whether hash join spills or not
	// In the future, we should calculate it based on the number of memory-intensive operators and statement memory available
	CCost costLocal(0);
    
    // inner tuples fit in memory
	if (dRowsInner * dWidthInner <= dHJSpillingMemThreshold)
	{	
		// hash join cost contains four parts:
		// 1. build hash table with inner tuples. This part is correlated with rows and width of
		// inner tuples and the number of columns used in join condition.
		// 2. feeding outer tuples. This part is correlated with rows and width of outer tuples
		// and the number of columns used.
		// 3. matching inner tuples. This part is correlated with rows and width of inner tuples.
		// 4. output tuples. This part is correlated with outer rows and width of the join result.  
		costLocal = CCost(pci->DRebinds() * (
			// cost of building hash table
			dRowsInner * (
				ulColsUsed * dHJHashTableColumnCostUnit
				+
				dWidthInner * dHJHashTableWidthCostUnit)
			+
			// cost of feeding outer tuples
			ulColsUsed * dRowsOuter * dJoinFeedingTupColumnCostUnit
				+ dWidthOuter * dRowsOuter * dJoinFeedingTupWidthCostUnit
			+
			// cost of matching inner tuples
			dWidthInner * dRowsInner * dHJHashingTupWidthCostUnit
			+
			// cost of output tuples
			pci->DRows() * pci->DWidth() * dJoinOutputTupCostUnit
			));
	}
	else
	{
        // inner tuples spill

		// hash join cost if spilling is the same as the non-spilling case, except that
		// parameter values are different. 
		costLocal = CCost(pci->DRebinds() * (
			dHJHashTableInitCostFactor + dRowsInner * (
				ulColsUsed * dHJHashTableColumnCostUnit
				+
				dWidthInner * dHJHashTableWidthCostUnit)
			+
			ulColsUsed * dRowsOuter * dHJFeedingTupColumnSpillingCostUnit
				+ dWidthOuter * dRowsOuter * dHJFeedingTupWidthSpillingCostUnit
			+
			dWidthInner * dRowsInner * dHJHashingTupWidthSpillingCostUnit
			+
			pci->DRows() * pci->DWidth() * dJoinOutputTupCostUnit
			));
	}
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costChild + costLocal;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostInnerIndexNLJoin
//
//	@doc:
//		Cost of inner index-nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostInnerIndexNLJoin
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT	(COperator::EopPhysicalInnerIndexNLJoin == exprhdl.Pop()->Eopid());

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];

	const CDouble dJoinFeedingTupColumnCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)->DVal();
	const CDouble dJoinFeedingTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)->DVal();
	const CDouble dJoinOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)->DVal();
	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond= exprhdl.PexprScalarChild(2);
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprJoinCond->PdpDerive())->PcrsUsed();
	const ULONG ulColsUsed = pcrsUsed->CElements();

	// cost of Index apply contains three parts:
	// 1. feeding outer tuples. This part is correlated with rows and width of outer tuples
	// and the number of columns used.
	// 2. repetitive index scan of inner side for each feeding tuple. This part of cost is
	// calculated and counted in its index scan child node.
	// 3. output tuples. This part is correlated with outer rows and width of the join result.
	CCost costLocal = CCost(pci->DRebinds() * (
		// cost of feeding outer tuples
		ulColsUsed * dRowsOuter * dJoinFeedingTupColumnCostUnit
			+ dWidthOuter * dRowsOuter * dJoinFeedingTupWidthCostUnit
		+
		// cost of output tuples
		pci->DRows() * pci->DWidth() * dJoinOutputTupCostUnit
		));

	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	ULONG ulRisk = pci->Pcstats()->UlStatsEstimationRisk();
	ULONG ulPenalizationFactor = 1;
	const CDouble dIndexJoinAllowedRiskThreshold =
			pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold)->DVal();
	if (dIndexJoinAllowedRiskThreshold < ulRisk)
	{
		ulPenalizationFactor = ulRisk;
	}

	return CCost(ulPenalizationFactor * (costLocal + costChild));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostNLJoin
//
//	@doc:
//		Cost of nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostNLJoin
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(CUtils::FNLJoin(exprhdl.Pop()));

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];
	const DOUBLE dRowsInner = pci->PdRows()[1];
	const DOUBLE dWidthInner = pci->PdWidth()[1];

	const CDouble dJoinFeedingTupColumnCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupColumnCostUnit)->DVal();
	const CDouble dJoinFeedingTupWidthCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinFeedingTupWidthCostUnit)->DVal();
	const CDouble dJoinOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpJoinOutputTupCostUnit)->DVal();
	const CDouble dInitScan = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)->DVal();
	const CDouble dTableScanCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTableScanCostUnit)->DVal();
	const CDouble dFilterColCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpFilterColCostUnit)->DVal();
	const CDouble dOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)->DVal();
	const CDouble dNLJFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor)->DVal();

	GPOS_ASSERT(0 < dJoinFeedingTupColumnCostUnit);
	GPOS_ASSERT(0 < dJoinFeedingTupWidthCostUnit);
	GPOS_ASSERT(0 < dJoinOutputTupCostUnit);
	GPOS_ASSERT(0 < dInitScan);
	GPOS_ASSERT(0 < dTableScanCostUnit);
	GPOS_ASSERT(0 < dFilterColCostUnit);
	GPOS_ASSERT(0 < dOutputTupCostUnit);
	GPOS_ASSERT(0 < dNLJFactor);

	// get the number of columns used in join condition
	CExpression *pexprJoinCond= exprhdl.PexprScalarChild(2);
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprJoinCond->PdpDerive())->PcrsUsed();
	const ULONG ulColsUsed = pcrsUsed->CElements();

	// cost of nested loop join contains three parts:
	// 1. feeding outer tuples. This part is correlated with rows and width of outer tuples
	// and the number of columns used.
	// 2. repetitive scan of inner side for each feeding tuple. This part of cost consists of
	// the following:
	// a. repetitive scan and filter of the materialized inner side
	// b. extract matched inner side tuples
	// with the cardinality of outer tuples, rows and width of the materialized inner side.
	// 3. output tuples. This part is correlated with outer rows and width of the join result.
	CCost costLocal = CCost(pci->DRebinds() * (
		// cost of feeding outer tuples
		ulColsUsed * dRowsOuter * dJoinFeedingTupColumnCostUnit
			+ dWidthOuter * dRowsOuter * dJoinFeedingTupWidthCostUnit
		+
		// cost of repetitive table scan of inner side
		dInitScan + dRowsOuter * (
			// cost of scan of inner side
			dRowsInner * dWidthInner * dTableScanCostUnit
			+
			// cost of filter of inner side
			dRowsInner * ulColsUsed * dFilterColCostUnit
			)
			// cost of extracting matched inner side
			+ pci->DRows() * dWidthInner * dOutputTupCostUnit
		+
		// cost of output tuples
		pci->DRows() * pci->DWidth() * dJoinOutputTupCostUnit
		));

	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	CCost costTotal = CCost(costLocal + costChild);

	// amplify NLJ cost based on NLJ factor and stats estimation risk
	// we don't want to penalize index join compared to nested loop join, so we make sure
	// that every time index join is penalized, we penalize nested loop join by at least the
	// same amount
	CDouble dPenalization = dNLJFactor;
	const CDouble dRisk(pci->Pcstats()->UlStatsEstimationRisk());
	if (dRisk > dPenalization)
	{
		const CDouble dIndexJoinAllowedRiskThreshold =
				pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpIndexJoinAllowedRiskThreshold)->DVal();
		if (dIndexJoinAllowedRiskThreshold < dRisk)
		{
			dPenalization = dRisk;
		}
	}

	return CCost(costTotal * dPenalization);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostMotion
//
//	@doc:
//		Cost of motion
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostMotion
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalMotionGather == eopid ||
			COperator::EopPhysicalMotionBroadcast == eopid ||
			COperator::EopPhysicalMotionHashDistribute == eopid ||
			COperator::EopPhysicalMotionRandom == eopid ||
			COperator::EopPhysicalMotionRoutedDistribute == eopid);

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];
	
	// motion cost contains three parts: sending cost, interconnect cost, and receiving cost.
	// TODO 2014-03-18
	// in current cost model, interconnect cost is tied with receiving cost. Because we 
	// only have one set calibration results in the dimension of the number of segments. 
	// Once we calibrate the cost model with different number of segments, I will update
	// the function.

	CDouble dSendCostUnit(0);
	CDouble dRecvCostUnit(0);
	CDouble recvCost(0);

	CCost costLocal(0);
	if (COperator::EopPhysicalMotionBroadcast == eopid)
	{
		// broadcast cost is amplified by the number of segments
		dSendCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBroadcastSendCostUnit)->DVal();
		dRecvCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBroadcastRecvCostUnit)->DVal();

		recvCost = dRowsOuter * dWidthOuter * pcmgpdb->UlHosts() * dRecvCostUnit;
	}
	else if (COperator::EopPhysicalMotionHashDistribute == eopid ||
			COperator::EopPhysicalMotionRandom == eopid ||
			COperator::EopPhysicalMotionRoutedDistribute == eopid)
	{
		dSendCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpRedistributeSendCostUnit)->DVal();
		dRecvCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpRedistributeRecvCostUnit)->DVal();

		// Adjust the cost of no-op hashed distribution to correctly reflect that no tuple movement is needed
		CPhysicalMotion *pMotion = CPhysicalMotion::PopConvert(exprhdl.Pop());
		CDistributionSpec *pds = pMotion->Pds();
		if (CDistributionSpec::EdtHashedNoOp == pds->Edt())
		{
			// promote the plan with redistribution on same distributed columns of base table for parallel append
			dSendCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNoOpCostUnit)->DVal();
			dRecvCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNoOpCostUnit)->DVal();
		}

		recvCost = pci->DRows() * pci->DWidth() * dRecvCostUnit;
	}
	else if (COperator::EopPhysicalMotionGather == eopid)
	{
		dSendCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpGatherSendCostUnit)->DVal();
		dRecvCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpGatherRecvCostUnit)->DVal();

		recvCost = dRowsOuter * dWidthOuter * pcmgpdb->UlHosts() * dRecvCostUnit;
	}

	GPOS_ASSERT(0 <= dSendCostUnit);
	GPOS_ASSERT(0 <= dRecvCostUnit);

	costLocal = CCost(pci->DRebinds() * (
		dRowsOuter * dWidthOuter * dSendCostUnit
		+
		recvCost
	));

	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostSequenceProject
//
//	@doc:
//		Cost of sequence project
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostSequenceProject
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequenceProject == exprhdl.Pop()->Eopid());

	const DOUBLE dRowsOuter = pci->PdRows()[0];
	const DOUBLE dWidthOuter = pci->PdWidth()[0];

	ULONG ulSortCols = 0;
	DrgPos *pdrgpos = CPhysicalSequenceProject::PopConvert(exprhdl.Pop())->Pdrgpos();
	const ULONG ulOrderSpecs = pdrgpos->UlLength();
	for (ULONG ul = 0; ul < ulOrderSpecs; ul++)
	{
		COrderSpec *pos = (*pdrgpos)[ul];
		ulSortCols += pos->UlSortColumns();
	}

	const CDouble dTupDefaultProcCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTupDefaultProcCostUnit)->DVal();
	GPOS_ASSERT(0 < dTupDefaultProcCostUnit);

	// we process (sorted window of) input tuples to compute window function values
	CCost costLocal = CCost(pci->DRebinds() * (ulSortCols * dRowsOuter * dWidthOuter * dTupDefaultProcCostUnit));
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostIndexScan
//
//	@doc:
//		Cost of index scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostIndexScan
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator *pop = exprhdl.Pop();
	COperator::EOperatorId eopid = pop->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalIndexScan == eopid ||
			COperator::EopPhysicalDynamicIndexScan == eopid);

	const CDouble dRandomIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpRandomIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dRandomIOBandwidth);

	const CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	const CDouble dInitScan = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)->DVal();
	const CDouble dTableWidth = CPhysicalScan::PopConvert(pop)->PstatsBaseTable()->DWidth();

	const CDouble dIndexFilterCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpIndexFilterCostUnit)->DVal();
	const CDouble dIndexScanTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpIndexScanTupCostUnit)->DVal();
	const CDouble dIndexScanTupRandomFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpIndexScanTupRandomFactor)->DVal();
	const CDouble dFilterColCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpFilterColCostUnit)->DVal();
	const CDouble dOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)->DVal();
	GPOS_ASSERT(0 < dIndexFilterCostUnit);
	GPOS_ASSERT(0 < dIndexScanTupCostUnit);
	GPOS_ASSERT(0 < dIndexScanTupRandomFactor);
	GPOS_ASSERT(0 < dFilterColCostUnit);
	GPOS_ASSERT(0 < dOutputTupCostUnit);	

	CDouble dRowsIndex = pci->DRows();

	ULONG ulIndexKeys = 1;
	if (COperator::EopPhysicalIndexScan == eopid)
	{
		ulIndexKeys = CPhysicalIndexScan::PopConvert(pop)->Pindexdesc()->UlKeys();
	}
	else
	{
		ulIndexKeys = CPhysicalDynamicIndexScan::PopConvert(pop)->Pindexdesc()->UlKeys();
	}

	// TODO: 2014-02-01
	// Add logic to judge if the index column used in the filter is the first key of a multi-key index or not.
	// and separate the cost functions for the two cases.

	// index scan cost contains two parts: index-column lookup and output tuple cost.
	// 1. index-column lookup: correlated with index lookup rows, the number of index columns used in lookup,
	// table width and a randomIOFactor.
	// 2. output tuple cost: this is handled by the Filter on top of IndexScan, if no Filter exists, we add output cost
	// when we sum-up children cost
	return CCost
			(
				pci->DRebinds() *
				(
				dRowsIndex *
				(ulIndexKeys * dIndexFilterCostUnit + dTableWidth * dIndexScanTupCostUnit + dIndexScanTupRandomFactor)
				)
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostBitmapTableScan
//
//	@doc:
//		Cost of bitmap table scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostBitmapTableScan
	(
	IMemoryPool *,  // pmp,
	CExpressionHandle & exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalBitmapTableScan == exprhdl.Pop()->Eopid() ||
		 COperator::EopPhysicalDynamicBitmapTableScan == exprhdl.Pop()->Eopid());

	CDouble dSeqIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpSeqIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	// TODO: ; 2014-04-11; compute the real cost here
//	return CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth()) / dSeqIOBandwidth * 0.5);

	CExpression *pexprIndexCond = exprhdl.PexprScalarChild(1 /*ulChildIndex*/);
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprIndexCond->PdpDerive())->PcrsUsed();

	if (COperator::EopScalarBitmapIndexProbe != pexprIndexCond->Pop()->Eopid() || 1 < pcrsUsed->CElements())
	{
		// child is Bitmap AND/OR, or we use Multi column index
		const CDouble dRandomIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpRandomIOBandwidth)->DVal();
		GPOS_ASSERT(0 < dRandomIOBandwidth);

		// TODO: ; 2014-04-11; compute the real cost here
		return CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth()) / dRandomIOBandwidth);
	}

	// if the expression is const table get, the pcrsUsed is empty
	// so we use minimum value DMinDistinct for dNDV in that case.
	CDouble dNDV = CHistogram::DMinDistinct;
	if (1 == pcrsUsed->CElements())
	{
		CColRef *pcrIndexCond =  pcrsUsed->PcrFirst();
		GPOS_ASSERT(NULL != pcrIndexCond);
		dNDV = pci->Pcstats()->DNDV(pcrIndexCond);
	}
	CDouble dNDVThreshold = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBitmapNDVThreshold)->DVal();

	if (dNDVThreshold <= dNDV)
	{
		return CostBitmapLargeNDV(pcmgpdb, pci, dNDV);
	}

	return CostBitmapSmallNDV(pcmgpdb, pci, dNDV);
}



//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostBitmapSmallNDV
//
//	@doc:
//		Cost of scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostBitmapSmallNDV
	(
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci,
	CDouble dNDV
	)
{
	const DOUBLE dRows = pci->DRows();
	const DOUBLE dWidth = pci->DWidth();

	CDouble dSize = (dRows * dWidth) * 0.001;

	CDouble dBitmapIO = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBitmapIOCostSmallNDV)->DVal();
	CDouble dBitmapPageCost = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBitmapPageCostSmallNDV)->DVal();

	return CCost(pci->DRebinds() * (dBitmapIO * dSize + dBitmapPageCost * dNDV));
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostBitmapLargeNDV
//
//	@doc:
//		Cost of when NDV is large
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostBitmapLargeNDV
	(
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci,
	CDouble dNDV
	)
{
	const DOUBLE dRows = pci->DRows();
	const DOUBLE dWidth = pci->DWidth();

	CDouble dSize = (dRows * dWidth * dNDV) * 0.001;
	CDouble dBitmapIO = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBitmapIOCostLargeNDV)->DVal();
	CDouble dBitmapPageCost = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpBitmapPageCostLargeNDV)->DVal();

	return CCost(pci->DRebinds() * (dBitmapIO * dSize + dBitmapPageCost * dNDV));
}

//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostScan
//
//	@doc:
//		Cost of scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostScan
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator *pop = exprhdl.Pop();
	COperator::EOperatorId eopid = pop->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalTableScan == eopid ||
				COperator::EopPhysicalDynamicTableScan == eopid ||
				COperator::EopPhysicalExternalScan == eopid);

	const CDouble dSeqIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpSeqIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	const CDouble dInitScan = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpInitScanFactor)->DVal();
	const CDouble dTableWidth = CPhysicalScan::PopConvert(pop)->PstatsBaseTable()->DWidth();
	
	// Get total rows for each host to scan
	const CDouble dTableScanCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTableScanCostUnit)->DVal();
	GPOS_ASSERT(0 < dTableScanCostUnit);

	switch (eopid)
	{
		case COperator::EopPhysicalTableScan:
		case COperator::EopPhysicalDynamicTableScan:
		case COperator::EopPhysicalExternalScan:
			// table scan cost considers only retrieving tuple cost,
			// since we scan the entire table here, the cost is correlated with table rows and table width,
			// since Scan's parent operator may be a filter that will be pushed into Scan node in GPDB plan,
			// we add Scan output tuple cost in the parent operator and not here
			return CCost(pci->DRebinds() * (dInitScan + pci->DRows() * dTableWidth * dTableScanCostUnit));
		default:
			GPOS_ASSERT(!"invalid index scan");
			return CCost(0);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::CostFilter
//
//	@doc:
//		Cost of filter
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::CostFilter
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	const CCostModelGPDB *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalFilter == exprhdl.Pop()->Eopid());

	const CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	const CDouble dOutputBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpOutputBandwidth)->DVal();
	GPOS_ASSERT(0 < dOutputBandwidth);

	const DOUBLE dInput = pci->PdRows()[0];
	const ULONG ulFilterCols = exprhdl.Pdpscalar(1 /*ulChildIndex*/)->PcrsUsed()->CElements();

	const CDouble dFilterColCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpFilterColCostUnit)->DVal();
	GPOS_ASSERT(0 < dFilterColCostUnit);
	const CDouble dOutputTupCostUnit = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpOutputTupCostUnit)->DVal();
	GPOS_ASSERT(0 < dOutputTupCostUnit);

	// filter cost is correlated with the input rows and the number of filter columns.
	CCost costLocal = CCost(dInput * ulFilterCols * dFilterColCostUnit);

	costLocal = CCost(costLocal.DVal() * pci->DRebinds());
	CCost costChild = CostChildren(pmp, exprhdl, pci, pcmgpdb->Pcp());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDB::Cost
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDB::Cost
	(
	CExpressionHandle &exprhdl, // handle gives access to expression properties
	const SCostingInfo *pci
	)
	const
{
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	if (FUnary(eopid))
	{
		return CostUnary(m_pmp, exprhdl, pci, m_pcp);
	}

	FnCost *pfnc = NULL;
	const ULONG ulSize = GPOS_ARRAY_SIZE(m_rgcm);

	// find the cost function corresponding to the given operator
	for (ULONG ul = 0; pfnc == NULL && ul < ulSize; ul++)
	{
		if (eopid == m_rgcm[ul].m_eopid)
		{
			pfnc = m_rgcm[ul].m_pfnc;
		}
	}
	GPOS_ASSERT(NULL != pfnc);

	return pfnc(m_pmp, exprhdl, this, pci);
}

// EOF
