//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalSpool.cpp
//
//	@doc:
//		Implementation of spool operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSpool.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::CPhysicalSpool
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSpool::CPhysicalSpool
	(
	IMemoryPool *pmp
	)
	:
	CPhysical(pmp)
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::~CPhysicalSpool
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSpool::~CPhysicalSpool()
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalSpool::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);

	return PcrsChildReqd(pmp, exprhdl, pcrsRequired, ulChildIndex,
						 gpos::ulong_max);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSpool::PosRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	COrderSpec *posRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	return PosPassThru(pmp, exprhdl, posRequired, ulChildIndex);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSpool::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	return PdsPassThru(pmp, exprhdl, pdsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalSpool::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG ulChildIndex,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(NULL != pppsRequired);
	
	return CPhysical::PppsRequiredPushThru(pmp, exprhdl, pppsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalSpool::PcteRequired
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &, //exprhdl,
	CCTEReq *pcter,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSpool::PrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl,
	CRewindabilitySpec *,// prsRequired,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// spool establishes rewindability on its own
	return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PosDerive
//
//	@doc:
//		Derive sort order
//
//--------------------------------------------------------------------------
COrderSpec *
CPhysicalSpool::PosDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PosDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PdsDerive
//
//	@doc:
//		Derive distribution
//
//--------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSpool::PdsDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PdsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//--------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSpool::PrsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	// rewindability of output is always true
	return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral /*ert*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSpool::FMatch
	(
	COperator *pop
	)
	const
{
	// spool doesn't contain any members as of now
	return Eopid() == pop->Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSpool::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSpool::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder *
#ifdef GPOS_DEBUG
	peo
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());

	// spool is order-preserving, sort enforcers have already been added
	return CEnfdProp::EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSpool::EpetDistribution
	(
	CExpressionHandle &/*exprhdl*/,
	const CEnfdDistribution *
#ifdef GPOS_DEBUG
	ped
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	// spool is distribution-preserving,
	// distribution enforcers have already been added
	return CEnfdProp::EpetUnnecessary;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSpool::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSpool::EpetRewindability
	(
	CExpressionHandle &, // exprhdl
	const CEnfdRewindability * // per
	)
	const
{
	// no need for enforcing rewindability on output
	return CEnfdProp::EpetUnnecessary;
}


BOOL
CPhysicalSpool::FValidContext
	(
	IMemoryPool *,
	COptimizationContext *,
	DrgPoc *pdrgpocChild
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpocChild);
	GPOS_ASSERT(1 == pdrgpocChild->UlLength());

	COptimizationContext *pocChild = (*pdrgpocChild)[0];
	CCostContext *pccBest = pocChild->PccBest();
	GPOS_ASSERT(NULL != pccBest);

	// partition selections that happen outside of a physical spool does not do
	// any good on rescan: a physical spool blocks the rescan from the entire
	// subtree (in particular, any dynamic scan) underneath it. That means when
	// we have a dynamic scan under a spool, and a corresponding partition
	// selector outside the spool, we run the risk of materializing the wrong
	// results.

	// For example, the following plan is invalid because the partition selector
	// won't be able to influence inner side of the nested loop join as intended
	// ("blocked" by the spool):

	// +--CPhysicalMotionGather(master)
	//    +--CPhysicalInnerNLJoin
	//       |--CPhysicalPartitionSelector
	//       |  +--CPhysicalMotionBroadcast
	//       |     +--CPhysicalTableScan "foo" ("foo")
	//       |--CPhysicalSpool
	//       |  +--CPhysicalLeftOuterHashJoin
	//       |     |--CPhysicalDynamicTableScan "pt" ("pt")
	//       |     |--CPhysicalMotionHashDistribute
	//       |     |  +--CPhysicalTableScan "bar" ("bar")
	//       |     +--CScalarCmp (=)
	//       |        |--CScalarIdent "d" (19)
	//       |        +--CScalarIdent "dk" (9)
	//       +--CScalarCmp (<)
	//          |--CScalarIdent "a" (0)
	//          +--CScalarIdent "partkey" (10)

	CDrvdPropPlan *pdpplanChild = pccBest->Pdpplan();
	if (pdpplanChild->Ppim()->FContainsUnresolved())
	{
		return false;
	}
	return true;
}
// EOF

