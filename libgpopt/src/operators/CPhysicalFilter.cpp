//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalFilter.cpp
//
//	@doc:
//		Implementation of filter operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"

#include "gpopt/base/CDistributionSpecAny.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalFilter.h"
#include "gpopt/operators/CPredicateUtils.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::CPhysicalFilter
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalFilter::CPhysicalFilter
	(
	IMemoryPool *pmp
	)
	:
	CPhysical(pmp)
{
	// when Filter includes outer references, correlated execution has to be enforced,
	// in this case, we create two child optimization requests to guarantee correct evaluation of parameters
	// (1) Broadcast
	// (2) Singleton

	SetDistrRequests(2 /*ulDistrReqs*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::~CPhysicalFilter
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalFilter::~CPhysicalFilter()
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PcrsRequired
//
//	@doc:
//		Compute required output columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalFilter::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex && "Required properties can only be computed on the relational child");

	return PcrsChildReqd(pmp, exprhdl, pcrsRequired, ulChildIndex, 1 /*ulScalarIndex*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalFilter::PosRequired
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
//		CPhysicalFilter::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalFilter::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG ulOptReq
	)
	const
{
	if (CDistributionSpec::EdtAny == pdsRequired->Edt() &&
		CDistributionSpecAny::PdsConvert(pdsRequired)->FAllowOuterRefs())
	{
		// this situation arises when we have Filter on top of (Dynamic)IndexScan,
		// in this case, we impose no distribution requirements even with the presence of outer references,
		// the reason is that the Filter must be the inner child of IndexNLJoin and
		// we need to have outer references referring to join's outer child
		pdsRequired->AddRef();
		return pdsRequired;
	}

	return CPhysical::PdsUnary(pmp, exprhdl, pdsRequired, ulChildIndex, ulOptReq);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalFilter::PrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// If there are outer references in the Filter (but none coming from the
	// child), we can optimize by adding a materialize in between. However, if
	// there are outer references in the child, we should *not* add a materialize
	// here.  Otherwise the child will not get rescanned leading to wrong
	// results.
	if (exprhdl.FHasOuterRefs() && !exprhdl.FHasOuterRefs(0))
	{
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral);
	}

	return PrsPassThru(pmp, exprhdl, prsRequired, ulChildIndex);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalFilter::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG 
#ifdef GPOS_DEBUG
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(NULL != pppsRequired);
	
	CPartIndexMap *ppimReqd = pppsRequired->Ppim();
	CPartFilterMap *ppfmReqd = pppsRequired->Ppfm();
	
	DrgPul *pdrgpul = ppimReqd->PdrgpulScanIds(pmp);
	
	CPartIndexMap *ppimResult = GPOS_NEW(pmp) CPartIndexMap(pmp);
	CPartFilterMap *ppfmResult = GPOS_NEW(pmp) CPartFilterMap(pmp);
	
	/// get derived part consumers
	CPartInfo *ppartinfo = exprhdl.Pdprel(0)->Ppartinfo();
	
	const ULONG ulPartIndexIds = pdrgpul->UlLength();
	BOOL fUseConstraints = (1 == exprhdl.Pdprel()->UlJoinDepth());
	
	for (ULONG ul = 0; ul < ulPartIndexIds; ul++)
	{
		ULONG ulPartIndexId = *((*pdrgpul)[ul]);

		if (!ppartinfo->FContainsScanId(ulPartIndexId))
		{
			// part index id does not exist in child nodes: no need to push through
			// the request
			continue;
		}

		ppimResult->AddRequiredPartPropagation(ppimReqd, ulPartIndexId, CPartIndexMap::EppraPreservePropagators);
		
		// look for a filter on the part key
		CExpression *pexprScalar = exprhdl.PexprScalarChild(1 /*ulChildIndex*/);

		CExpression *pexprCmp = NULL;
		DrgPpartkeys *pdrgppartkeys = ppimReqd->Pdrgppartkeys(ulPartIndexId);
		const ULONG ulKeysets = pdrgppartkeys->UlLength();
		for (ULONG ulKey = 0; NULL == pexprCmp && ulKey < ulKeysets; ulKey++)
		{
			// get partition key
			DrgDrgPcr *pdrgpdrgpcrPartKeys = (*pdrgppartkeys)[ulKey]->Pdrgpdrgpcr();

			// try to generate a request with dynamic partition selection		
			pexprCmp = CPredicateUtils::PexprExtractPredicatesOnPartKeys
									(
									pmp,
									pexprScalar,
									pdrgpdrgpcrPartKeys,
									NULL, /*pcrsAllowedRefs*/
									fUseConstraints
									);
		}
				
		if (NULL == pexprCmp)
		{
			// no comparison found in filter: check if a comparison was already
			// specified in the required partition propagation
			if (ppfmReqd->FContainsScanId(ulPartIndexId))
			{
				pexprCmp = ppfmReqd->Pexpr(ulPartIndexId);
				pexprCmp->AddRef();
			}
			
			// TODO:  - May 31, 2012; collect multiple comparisons on the 
			// partition keys
		}
		
		if (NULL != pexprCmp)
		{
			// interesting filter found
			ppfmResult->AddPartFilter(pmp, ulPartIndexId, pexprCmp, NULL /*pstats */);
		}
	}
	
	pdrgpul->Release();

	return GPOS_NEW(pmp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalFilter::PcteRequired
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
//		CPhysicalFilter::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalFilter::PosDerive
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
//		CPhysicalFilter::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalFilter::PdsDerive
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
//		CPhysicalFilter::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalFilter::PrsDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PrsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalFilter::FMatch
	(
	COperator *pop
	)
	const
{
	// filter doesn't contain any members as of now
	return Eopid() == pop->Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalFilter::FProvidesReqdCols
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
//		CPhysicalFilter::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalFilter::EpetOrder
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

	// always force sort to be on top of filter
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalFilter::EpetDistribution
	(
	CExpressionHandle &exprhdl,
	const CEnfdDistribution *ped
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	// get distribution delivered by the filter node
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
	if (ped->FCompatible(pds))
	{
	 	// required distribution is already provided
	 	return CEnfdProp::EpetUnnecessary;
	}

	// required distribution will be enforced on Filter's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalFilter::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalFilter::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	// get rewindability delivered by the Filter node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		 // required rewindability is already provided
		 return CEnfdProp::EpetUnnecessary;
	}

	// always force spool to be on top of filter
	return CEnfdProp::EpetRequired;
}


// EOF

