//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalNLJoin.cpp
//
//	@doc:
//		Implementation of base nested-loops join operator
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

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CPhysicalNLJoin.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CPhysicalCorrelatedInnerNLJoin.h"
#include "gpopt/operators/CPhysicalCorrelatedLeftOuterNLJoin.h"
#include "gpopt/operators/CPhysicalCorrelatedInLeftSemiNLJoin.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::CPhysicalNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalNLJoin::CPhysicalNLJoin
	(
	IMemoryPool *pmp
	)
	:
	CPhysicalJoin(pmp)
{
	// NLJ creates two partition propagation requests for children:
	// (0) push possible Dynamic Partition Elimination (DPE) predicates from join's predicate to
	//		outer child, since outer child executes first
	// (1) ignore DPE opportunities in join's predicate, and push incoming partition propagation
	//		request to both children,
	//		this request handles the case where the inner child needs to be broadcasted, which prevents
	//		DPE by outer child since a Motion operator gets in between PartitionSelector and DynamicScan

	SetPartPropagateRequests(2);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::~CPhysicalNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalNLJoin::~CPhysicalNLJoin()
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalNLJoin::PosRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	COrderSpec *posInput,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(ulChildIndex < 2 &&
				"Required sort order can be computed on the relational child only");

	if (0 == ulChildIndex)
	{
		return PosPropagateToOuter(pmp, exprhdl, posInput);
	}

	return GPOS_NEW(pmp) COrderSpec(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalNLJoin::PrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG ulChildIndex,
	DrgPdp *, //pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(ulChildIndex < 2 &&
				"Required rewindability can be computed on the relational child only");

	// if there are outer references, then we need a materialize on both children
	if (exprhdl.FHasOuterRefs())
	{
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral);
	}

	if (1 == ulChildIndex)
	{
		// inner child has to be rewindable
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral /*ert*/);
	}

	// pass through requirements to outer child
	return PrsPassThru(pmp, exprhdl, prsRequired, 0 /*ulChildIndex*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalNLJoin::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(ulChildIndex < 2 &&
				"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pcrsRequired);

	// For subqueries in the projection list, the required columns from the outer child
	// are often pushed down to the inner child and are not visible at the top level
	// so we can use the outer refs of the inner child as required from outer child
	if (0 == ulChildIndex)
	{
		CColRefSet *pcrsOuter = exprhdl.Pdprel(1)->PcrsOuter();
		pcrs->Include(pcrsOuter);
	}

	// request inner child of correlated join to provide required inner columns
	if (1 == ulChildIndex && FCorrelated())
	{
		pcrs->Include(PdrgPcrInner());
	}

	CColRefSet *pcrsReqd = PcrsChildReqd(pmp, exprhdl, pcrs, ulChildIndex, 2 /*ulScalarIndex*/);
	pcrs->Release();

	return pcrsReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator;
//
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalNLJoin::EpetOrder
	(
	CExpressionHandle &exprhdl,
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());

	if (FSortColsInOuterChild(m_pmp, exprhdl, peo->PosRequired()))
	{
		return CEnfdProp::EpetOptional;
	}

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalNLJoin::PppsRequiredNLJoinChild
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalNLJoin::PppsRequiredNLJoinChild
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG ulChildIndex,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG ulOptReq
	)
{
	GPOS_ASSERT(NULL != pppsRequired);

	if (1 == ulOptReq)
	{
		// request (1): push partition propagation requests to join's children,
		// do not consider possible dynamic partition elimination using join predicate here,
		// this is handled by optimization request (0) below
		return CPhysical::PppsRequiredPushThruNAry(pmp, exprhdl, pppsRequired, ulChildIndex);
	}
	GPOS_ASSERT(0 == ulOptReq);

	CPartIndexMap *ppim = pppsRequired->Ppim();
	CPartFilterMap *ppfm = pppsRequired->Ppfm();

	DrgPul *pdrgpul = ppim->PdrgpulScanIds(pmp);
	
	CPartIndexMap *ppimResult = GPOS_NEW(pmp) CPartIndexMap(pmp);
	CPartFilterMap *ppfmResult = GPOS_NEW(pmp) CPartFilterMap(pmp);

	CPartInfo *ppartinfoOuter = exprhdl.Pdprel(0)->Ppartinfo();

	CColRefSet *pcrsOutputOuter = exprhdl.Pdprel(0)->PcrsOutput();
	CColRefSet *pcrsOutputInner = exprhdl.Pdprel(1)->PcrsOutput();

	const ULONG ulPartIndexIds = pdrgpul->UlLength();
		
	for (ULONG ul = 0; ul < ulPartIndexIds; ul++)
	{
		ULONG ulPartIndexId = *((*pdrgpul)[ul]);
		if (ppfm->FContainsScanId(ulPartIndexId))
		{
			GPOS_ASSERT(NULL != ppfm->Pexpr(ulPartIndexId));
			// a selection-based propagation request pushed from above: do not propagate any
			// further as the join will reduce cardinality and thus may select more partitions
			// for scanning
			continue;
		}
		
		BOOL fOuterPartConsumer = ppartinfoOuter->FContainsScanId(ulPartIndexId);

		// in order to find interesting join predicates that can be used for DPE,
		// one side of the predicate must be the partition key, while the other side must only contain
		// references from the join child that does not have the partition consumer
		CColRefSet *pcrsAllowedRefs = pcrsOutputOuter;
		if (fOuterPartConsumer)
		{
			pcrsAllowedRefs = pcrsOutputInner;
		}

		if (0 == ulChildIndex && fOuterPartConsumer)
		{
			// always push through required partition propagation for consumers on the
			// outer side of the nested loop join
			DrgPpartkeys *pdrgppartkeys = ppartinfoOuter->PdrgppartkeysByScanId(ulPartIndexId);
			GPOS_ASSERT(NULL != pdrgppartkeys);
			pdrgppartkeys->AddRef();

			ppimResult->AddRequiredPartPropagation(ppim, ulPartIndexId, CPartIndexMap::EppraPreservePropagators, pdrgppartkeys);
		}
		else
		{
			// check if there is an interesting condition involving the partition key
			CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*ulChildIndex*/);
			AddFilterOnPartKey(pmp, true /*fNLJoin*/, pexprScalar, ppim, ppfm, ulChildIndex, ulPartIndexId, fOuterPartConsumer, ppimResult, ppfmResult, pcrsAllowedRefs);
		}
	}
	
	pdrgpul->Release();

	return GPOS_NEW(pmp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}


// EOF

