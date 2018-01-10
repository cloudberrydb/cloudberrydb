//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CCostModelGPDBLegacy.cpp
//
//	@doc:
//		Implementation of GPDB's legacy cost model
//---------------------------------------------------------------------------

#include "gpopt/base/COrderSpec.h"
#include "gpopt/base/CWindowFrame.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSequenceProject.h"

#include "gpdbcost/CCostModelGPDBLegacy.h"

using namespace gpos;
using namespace gpdbcost;


// initialization of cost functions
const CCostModelGPDBLegacy::SCostMapping CCostModelGPDBLegacy::m_rgcm[] =
{
	{COperator::EopPhysicalTableScan, CostScan},
	{COperator::EopPhysicalDynamicTableScan, CostScan},
	{COperator::EopPhysicalExternalScan, CostScan},

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

	{COperator::EopPhysicalInnerHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftSemiHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftAntiSemiHashJoin, CostHashJoin},
	{COperator::EopPhysicalLeftAntiSemiHashJoinNotIn, CostHashJoin},
	{COperator::EopPhysicalLeftOuterHashJoin, CostHashJoin},

	{COperator::EopPhysicalInnerIndexNLJoin, CostIndexNLJoin},
	{COperator::EopPhysicalLeftOuterIndexNLJoin, CostIndexNLJoin},

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
//		CCostModelGPDBLegacy::CCostModelGPDBLegacy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CCostModelGPDBLegacy::CCostModelGPDBLegacy
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

	m_pcp = GPOS_NEW(pmp) CCostModelParamsGPDBLegacy(pmp);
	SetParams(pdrgpcp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::DRowsPerHost
//
//	@doc:
//		Return number of rows per host
//
//---------------------------------------------------------------------------
CDouble
CCostModelGPDBLegacy::DRowsPerHost
	(
	CDouble dRowsTotal
	)
	const
{
	return dRowsTotal / m_ulSegments;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::~CCostModelGPDBLegacy
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CCostModelGPDBLegacy::~CCostModelGPDBLegacy()
{
	m_pcp->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostTupleProcessing
//
//	@doc:
//		Cost of tuple processing
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostTupleProcessing
	(
	DOUBLE dRows,
	DOUBLE dWidth,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pcp);

	CDouble dTupProcBandwidth = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	return CCost((dRows * dWidth) / dTupProcBandwidth);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostUnary
//
//	@doc:
//		Helper function to return cost of a plan rooted by unary operator
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostUnary
	(
	DOUBLE dRows,
	DOUBLE dWidth,
	DOUBLE dRebinds,
	DOUBLE *pdcostChildren,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pcp);

	CCost costLocal = CCost(dRebinds * CostTupleProcessing(dRows, dWidth, pcp).DVal());
	CCost costChild = CostSum(pdcostChildren, 1 /*ulSize*/);

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSpooling
//
//	@doc:
//		Helper function to compute spooling cost
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSpooling
	(
	DOUBLE dRows,
	DOUBLE dWidth,
	DOUBLE dRebinds,
	DOUBLE *pdcostChildren,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pcp);

	return CostUnary(dRows, dWidth, dRebinds, pdcostChildren, pcp);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostRedistribute
//
//	@doc:
//		Cost of various redistribute motion operators
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostRedistribute
	(
	DOUBLE dRows,
	DOUBLE dWidth,
	ICostModelParams *pcp
	)
{
	GPOS_ASSERT(NULL != pcp);

	CDouble dNetBandwidth = pcp->PcpLookup(CCostModelParamsGPDBLegacy::EcpNetBandwidth)->DVal();
	GPOS_ASSERT(0 < dNetBandwidth);

	return CCost(dRows * dWidth / dNetBandwidth);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::FUnary
//
//	@doc:
//		Check if given operator is unary
//
//---------------------------------------------------------------------------
BOOL
CCostModelGPDBLegacy::FUnary
	(
	COperator::EOperatorId eopid
	)
{
	return COperator::EopPhysicalAssert == eopid ||
			 COperator::EopPhysicalComputeScalar == eopid ||
			 COperator::EopPhysicalFilter == eopid ||
			 COperator::EopPhysicalLimit == eopid ||
			 COperator::EopPhysicalPartitionSelector == eopid ||
			 COperator::EopPhysicalPartitionSelectorDML == eopid ||
			 COperator::EopPhysicalRowTrigger == eopid ||
			 COperator::EopPhysicalSplit == eopid ||
			 COperator::EopPhysicalSpool == eopid;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::Cost
//
//	@doc:
//		Add up array of costs
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSum
	(
	DOUBLE *pdCost,
	ULONG ulSize
	)
{
	GPOS_ASSERT(NULL != pdCost);

	DOUBLE dRes = 1.0;
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		dRes = dRes + pdCost[ul];
	}

	return CCost(dRes);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostCTEProducer
//
//	@doc:
//		Cost of CTE producer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostCTEProducer
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEProducer == exprhdl.Pop()->Eopid());

	CCost cost = CostUnary(pci->DRows(), pci->DWidth(), pci->DRebinds(), pci->PdCost(), pcmgpdb->Pcp());

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
	CCost costSpooling = CostSpooling(pci->DRows(), pci->DWidth(), pci->DRebinds(), pci->PdCost(), pcmgpdb->Pcp());

	return cost + costSpooling;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostCTEConsumer
//
//	@doc:
//		Cost of CTE consumer
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostCTEConsumer
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalCTEConsumer == exprhdl.Pop()->Eopid());

	CDouble dSeqIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	return CCost(pci->DRebinds() * pci->DRows() * pci->DWidth() / dSeqIOBandwidth);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostConstTableGet
//
//	@doc:
//		Cost of const table get
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostConstTableGet
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
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
//		CCostModelGPDBLegacy::CostDML
//
//	@doc:
//		Cost of DML
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostDML
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalDML == exprhdl.Pop()->Eopid());

	CDouble dTupUpdateBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupUpdateBandwith)->DVal();
	GPOS_ASSERT(0 < dTupUpdateBandwidth);

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	CCost costLocal = CCost(pci->DRebinds() * (dRowsOuter * dWidthOuter) / dTupUpdateBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostScalarAgg
//
//	@doc:
//		Cost of scalar agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostScalarAgg
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalScalarAgg == exprhdl.Pop()->Eopid());

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	// local cost is the cost of processing 1 tuple (size of output) +
	// cost of processing N tuples (size of input)
	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(pci->DRebinds() * (1.0 * pci->DWidth() + dRowsOuter * dWidthOuter) / dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostStreamAgg
//
//	@doc:
//		Cost of stream agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostStreamAgg
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
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

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth() + dRowsOuter * dWidthOuter) / dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSequence
//
//	@doc:
//		Cost of sequence
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSequence
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequence == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSort
//
//	@doc:
//		Cost of sort
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSort
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSort == exprhdl.Pop()->Eopid());

	// log operation below
	CDouble dRows = CDouble(std::max(1.0, pci->DRows()));
	CDouble dRebinds = CDouble(pci->DRebinds());
	CDouble dWidth = CDouble(pci->DWidth());
	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(dRebinds * (dRows * dRows.FpLog2() * dWidth * dWidth / dTupProcBandwidth));
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostTVF
//
//	@doc:
//		Cost of table valued function
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostTVF
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
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
//		CCostModelGPDBLegacy::CostUnionAll
//
//	@doc:
//		Cost of UnionAll
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostUnionAll
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSerialUnionAll == exprhdl.Pop()->Eopid());

	CCost costLocal = CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostHashAgg
//
//	@doc:
//		Cost of hash agg
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostHashAgg
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
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
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	CDouble dHashFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpHashFactor)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth() + dHashFactor * dRowsOuter * dWidthOuter) / dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostHashJoin
//
//	@doc:
//		Cost of hash join
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostHashJoin
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
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

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];
	DOUBLE dRowsInner = pci->PdRows()[1];
	DOUBLE dWidthInner = pci->PdWidth()[1];

	CDouble dHashFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpHashFactor)->DVal();
	CDouble dHashJoinFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpHJFactor)->DVal();
	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	CCost costLocal = CCost(pci->DRebinds() * (dRowsOuter * dWidthOuter + dRowsInner * dWidthInner * dHashFactor * dHashJoinFactor) / dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costChild + costLocal;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostIndexNLJoin
//
//	@doc:
//		Cost of inner or outer index-nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostIndexNLJoin
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	CCost costLocal = CCost(pci->DRebinds() * CostTupleProcessing(pci->DRows(), pci->DWidth(), pcmgpdb->Pcp()).DVal());
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostNLJoin
//
//	@doc:
//		Cost of nljoin
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostNLJoin
	(
	IMemoryPool *, // pmp
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(CUtils::FNLJoin(exprhdl.Pop()));

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];
	DOUBLE dRowsInner = pci->PdRows()[1];
	DOUBLE dWidthInner = pci->PdWidth()[1];

	CDouble dNLJFactor = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpNLJFactor)->DVal();
	CDouble dNLJOuterFactor =  pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpNLJOuterFactor)->DVal();
	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);
	GPOS_ASSERT(0 < dNLJOuterFactor);
	GPOS_ASSERT(0 < dNLJFactor);

	 CCost costLocal = CCost(pci->DRebinds() * (dRowsOuter * dWidthOuter * dRowsInner * dWidthInner * dNLJOuterFactor) / dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	CCost costTotal = CCost(costLocal + costChild);

	// amplify NLJ cost based on NLJ factor
	return CCost(costTotal * dNLJFactor);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostMotion
//
//	@doc:
//		Cost of motion
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostMotion
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDBLegacy *pcmgpdb,
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

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	CCost costLocal(0);
	if (COperator::EopPhysicalMotionBroadcast == eopid)
	{
		// broadcast cost is amplified by the number of segments
		CDouble dNetBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpNetBandwidth)->DVal();
		GPOS_ASSERT(0 < dNetBandwidth);

		costLocal = CCost(pci->DRebinds() * dRowsOuter * dWidthOuter * pcmgpdb->UlHosts() / dNetBandwidth);
	}
	else
	{
		costLocal = CCost(pci->DRebinds() * CostRedistribute(dRowsOuter, dWidthOuter, pcmgpdb->Pcp()).DVal());
	}
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostSequenceProject
//
//	@doc:
//		Cost of sequence project
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostSequenceProject
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalSequenceProject == exprhdl.Pop()->Eopid());

	DOUBLE dRowsOuter = pci->PdRows()[0];
	DOUBLE dWidthOuter = pci->PdWidth()[0];

	ULONG ulSortCols = 0;
	DrgPos *pdrgpos = CPhysicalSequenceProject::PopConvert(exprhdl.Pop())->Pdrgpos();
	const ULONG ulOrderSpecs = pdrgpos->UlLength();
	for (ULONG ul = 0; ul < ulOrderSpecs; ul++)
	{
		COrderSpec *pos = (*pdrgpos)[ul];
		ulSortCols += pos->UlSortColumns();
	}

	CDouble dTupProcBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpTupProcBandwidth)->DVal();
	GPOS_ASSERT(0 < dTupProcBandwidth);

	// we process (sorted window of) input tuples to compute window function values
	CCost costLocal = CCost(pci->DRebinds() * (ulSortCols * dRowsOuter * dWidthOuter) / dTupProcBandwidth);
	CCost costChild = CostSum(pci->PdCost(), pci->UlChildren());

	return costLocal + costChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostIndexScan
//
//	@doc:
//		Cost of index scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostIndexScan
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);

	COperator::EOperatorId eopid = exprhdl.Pop()->Eopid();
	GPOS_ASSERT(COperator::EopPhysicalIndexScan == eopid ||
			COperator::EopPhysicalDynamicIndexScan == eopid);

	CDouble dRandomIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpRandomIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dRandomIOBandwidth);

	switch (eopid)
	{
		case COperator::EopPhysicalDynamicIndexScan:
		case COperator::EopPhysicalIndexScan:
			return CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth()) / dRandomIOBandwidth);

		default:
			GPOS_ASSERT(!"invalid index scan");
			return CCost(0);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostBitmapTableScan
//
//	@doc:
//		Cost of bitmap table scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostBitmapTableScan
	(
	IMemoryPool *,  // pmp,
	CExpressionHandle &
#ifdef GPOS_DEBUG
	 exprhdl
#endif // GPOS_DEBUG
	,
	const CCostModelGPDBLegacy *pcmgpdb,
	const SCostingInfo *pci
	)
{
	GPOS_ASSERT(NULL != pcmgpdb);
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(COperator::EopPhysicalBitmapTableScan == exprhdl.Pop()->Eopid() ||
		 COperator::EopPhysicalDynamicBitmapTableScan == exprhdl.Pop()->Eopid());

	CDouble dSeqIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	// TODO: ; 2014-04-11; compute the real cost here
	return CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth()) / dSeqIOBandwidth * 0.5);
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::CostScan
//
//	@doc:
//		Cost of scan
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::CostScan
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl,
	const CCostModelGPDBLegacy *pcmgpdb,
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

	CDouble dSeqIOBandwidth = pcmgpdb->Pcp()->PcpLookup(CCostModelParamsGPDBLegacy::EcpSeqIOBandwidth)->DVal();
	GPOS_ASSERT(0 < dSeqIOBandwidth);

	switch (eopid)
	{
		case COperator::EopPhysicalTableScan:
		case COperator::EopPhysicalDynamicTableScan:
		case COperator::EopPhysicalExternalScan:
			return CCost(pci->DRebinds() * (pci->DRows() * pci->DWidth()) / dSeqIOBandwidth);

		default:
			GPOS_ASSERT(!"invalid index scan");
			return CCost(0);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CCostModelGPDBLegacy::Cost
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CCost
CCostModelGPDBLegacy::Cost
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
		return CostUnary(pci->DRows(), pci->DWidth(), pci->DRebinds(), pci->PdCost(), m_pcp);
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
