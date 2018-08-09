//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotion.cpp
//
//	@doc:
//		Implementation of motion operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotion.h"
#include "gpopt/search/CMemo.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::FValidContext
//
//	@doc:
//		Check if optimization context is valid
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotion::FValidContext
	(
	IMemoryPool *,
	COptimizationContext *poc,
	COptimizationContextArray *pdrgpocChild
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpocChild);
	GPOS_ASSERT(1 == pdrgpocChild->Size());

	COptimizationContext *pocChild = (*pdrgpocChild)[0];
	CCostContext *pccBest = pocChild->PccBest();
	GPOS_ASSERT(NULL != pccBest);

	CDrvdPropPlan *pdpplanChild = pccBest->Pdpplan();
	if (pdpplanChild->Ppim()->FContainsUnresolved())
	{
		return false;
	}

	CEnfdDistribution *ped = poc->Prpp()->Ped();
	if (ped->FCompatible(this->Pds()) && ped->FCompatible(pdpplanChild->Pds()))
	{
		// required distribution is compatible with the distribution delivered by Motion and its child plan,
		// in this case, Motion is redundant since child plan delivers the required distribution
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalMotion::PdsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &, // exprhdl
	CDistributionSpec *, // pdsRequired
	ULONG
#ifdef GPOS_DEBUG
	child_index
#endif // GPOS_DEBUG
	,
	CDrvdProp2dArray *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == child_index);

	// any motion operator is distribution-establishing and does not require
	// child to deliver any specific distribution
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalMotion::PrsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &, // exprhdl
	CRewindabilitySpec *, // prsRequired
	ULONG
#ifdef GPOS_DEBUG
	child_index
#endif // GPOS_DEBUG
	,
	CDrvdProp2dArray *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == child_index);

	// motion does not preserve rewindability;
	// child does not need to be rewindable
	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNotRewindable, CRewindabilitySpec::EmhtNoMotion);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalMotion::PppsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG 
#ifdef GPOS_DEBUG
	child_index
#endif // GPOS_DEBUG
	,
	CDrvdProp2dArray *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(0 == child_index);
	GPOS_ASSERT(NULL != pppsRequired);
	
	CPartIndexMap *ppimReqd = pppsRequired->Ppim();
	CPartFilterMap *ppfmReqd = pppsRequired->Ppfm();
	
	ULongPtrArray *pdrgpul = ppimReqd->PdrgpulScanIds(mp);
	
	CPartIndexMap *ppimResult = GPOS_NEW(mp) CPartIndexMap(mp);
	CPartFilterMap *ppfmResult = GPOS_NEW(mp) CPartFilterMap(mp);
	
	/// get derived part consumers
	CPartInfo *ppartinfo = exprhdl.GetRelationalProperties(0)->Ppartinfo();
	
	const ULONG ulPartIndexSize = pdrgpul->Size();
	
	for (ULONG ul = 0; ul < ulPartIndexSize; ul++)
	{
		ULONG part_idx_id = *((*pdrgpul)[ul]);

		if (!ppartinfo->FContainsScanId(part_idx_id))
		{
			// part index id does not exist in child nodes: do not push it below 
			// the motion
			continue;
		}

		ppimResult->AddRequiredPartPropagation(ppimReqd, part_idx_id, CPartIndexMap::EppraPreservePropagators);
		(void) ppfmResult->FCopyPartFilter(m_mp, part_idx_id, ppfmReqd);
	}
		
	pdrgpul->Release();

	return GPOS_NEW(mp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalMotion::PcteRequired
	(
	IMemoryPool *, //mp,
	CExpressionHandle &, //exprhdl,
	CCTEReq *pcter,
	ULONG
#ifdef GPOS_DEBUG
	child_index
#endif
	,
	CDrvdProp2dArray *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == child_index);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalMotion::PdsDerive
	(
	IMemoryPool */*mp*/,
	CExpressionHandle &/*exprhdl*/
	)
	const
{
	CDistributionSpec *pds = Pds();
	pds->AddRef();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalMotion::PrsDerive
	(
	IMemoryPool *mp,
	CExpressionHandle & // exprhdl
	)
	const
{
	// output of motion is non-rewindable and imposes a motion hazard
	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNotRewindable, CRewindabilitySpec::EmhtMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::EpetDistribution
//
//	@doc:
//		Return distribution property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotion::EpetDistribution
	(
	CExpressionHandle &, // exprhdl
	const CEnfdDistribution *ped
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	if (ped->FCompatible(Pds()))
	{
		return CEnfdProp::EpetUnnecessary;
	}

	return CEnfdProp::EpetProhibited;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotion::EpetRewindability
//
//	@doc:
//		Return rewindability property enforcing type for this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotion::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability * // per
	)
	const
{
	if (exprhdl.HasOuterRefs())
	{
		// motion has outer references: prohibit this plan 
		// Note: this is a GPDB restriction as Motion operators are push-based
		return CEnfdProp::EpetProhibited;
	}

	// motion does not provide rewindability on its output
	return CEnfdProp::EpetRequired;
}

// EOF
