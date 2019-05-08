//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalJoin.cpp
//
//	@doc:
//		Implementation of physical join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecReplicated.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::CPhysicalJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalJoin::CPhysicalJoin
	(
	IMemoryPool *mp
	)
	:
	CPhysical(mp)
{
	m_phmpp = GPOS_NEW(mp) PartPropReqToPartPropSpecMap(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::~CPhysicalJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalJoin::~CPhysicalJoin()
{
	m_phmpp->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::Matches
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::Matches
	(
	COperator *pop
	)
	const
{
	return Eopid() == pop->Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PosPropagateToOuter
//
//	@doc:
//		Helper for propagating required sort order to outer child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalJoin::PosPropagateToOuter
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	COrderSpec *posRequired
	)
{
	// propagate the order requirement to the outer child only if all the columns
	// specified by the order requirement come from the outer child
	CColRefSet *pcrs = posRequired->PcrsUsed(mp);
	BOOL fOuterSortCols = exprhdl.GetRelationalProperties(0)->PcrsOutput()->ContainsAll(pcrs);
	pcrs->Release();
	if (fOuterSortCols)
	{
		return PosPassThru(mp, exprhdl, posRequired, 0 /*child_index*/);
	}

	return GPOS_NEW(mp) COrderSpec(mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalJoin::PcrsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG child_index,
	CDrvdProp2dArray *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(child_index < 2 &&
				"Required properties can only be computed on the relational child");
	
	return PcrsChildReqd(mp, exprhdl, pcrsRequired, child_index, 2 /*ulScalarIndex*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalJoin::PppsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG child_index,
	CDrvdProp2dArray *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(NULL != pppsRequired);
	
	return CPhysical::PppsRequiredPushThruNAry(mp, exprhdl, pppsRequired, child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalJoin::PcteRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CCTEReq *pcter,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(2 > child_index);

	return PcterNAry(mp, exprhdl, pcter, child_index, pdrgpdpCtxt);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FProvidesReqdCols
//
//	@doc:
//		Helper for checking if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(3 == exprhdl.Arity());

	// union columns from relational children
	CColRefSet *pcrs = GPOS_NEW(m_mp) CColRefSet(m_mp);
	ULONG arity = exprhdl.Arity();
	for (ULONG i = 0; i < arity - 1; i++)
	{
		CColRefSet *pcrsChild = exprhdl.GetRelationalProperties(i)->PcrsOutput();
		pcrs->Union(pcrsChild);
	}

	BOOL fProvidesCols = pcrs->ContainsAll(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FSortColsInOuterChild
//
//	@doc:
//		Helper for checking if required sort columns come from outer child
//
//----------------------------------------------------------------------------
BOOL
CPhysicalJoin::FSortColsInOuterChild
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	COrderSpec *pos
	)
{
	GPOS_ASSERT(NULL != pos);

	CColRefSet *pcrsSort = pos->PcrsUsed(mp);
	CColRefSet *pcrsOuterChild = exprhdl.GetRelationalProperties(0 /*child_index*/)->PcrsOutput();
	BOOL fSortColsInOuter = pcrsOuterChild->ContainsAll(pcrsSort);
	pcrsSort->Release();

	return fSortColsInOuter;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FOuterProvidesReqdCols
//
//	@doc:
//		Helper for checking if the outer input of a binary join operator
//		includes the required columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FOuterProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired
	)
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(3 == exprhdl.Arity() && "expected binary join");

	CColRefSet *pcrsOutput = exprhdl.GetRelationalProperties(0 /*child_index*/)->PcrsOutput();

	return pcrsOutput->ContainsAll(pcrsRequired);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child;
//		this function creates a request for ANY distribution on the outer
//		child, then matches the delivered distribution on the inner child,
//		this results in sending either a broadcast or singleton distribution
//		requests to the inner child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalJoin::PdsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(2 > child_index);

	// if expression has to execute on a single host then we need a gather
	if (exprhdl.NeedsSingletonExecution())
	{
		return PdsRequireSingleton(mp, exprhdl, pdsRequired, child_index);
	}

	if (exprhdl.HasOuterRefs())
	{
		if (CDistributionSpec::EdtSingleton == pdsRequired->Edt() ||
			CDistributionSpec::EdtReplicated == pdsRequired->Edt())
		{
			return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
		}
		return GPOS_NEW(mp) CDistributionSpecReplicated();
	}

	if (1 == child_index)
	{
		// compute a matching distribution based on derived distribution of outer child
		CDistributionSpec *pdsOuter = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();

		if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
		{
			// first child is universal, request second child to execute on a single host to avoid duplicates
			return GPOS_NEW(mp) CDistributionSpecSingleton();
		}

		if (CDistributionSpec::EdtSingleton == pdsOuter->Edt() || 
			CDistributionSpec::EdtStrictSingleton == pdsOuter->Edt())
		{
			// require inner child to have matching singleton distribution
			return CPhysical::PdssMatching(mp, CDistributionSpecSingleton::PdssConvert(pdsOuter));
		}

		// otherwise, require inner child to be replicated
		return GPOS_NEW(mp) CDistributionSpecReplicated();
	}

	// no distribution requirement on the outer side
	return GPOS_NEW(mp) CDistributionSpecAny(this->Eopid());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalJoin::PdsDerive
	(
	IMemoryPool *, // mp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*child_index*/)->Pds();
	CDistributionSpec *pdsInner = exprhdl.Pdpplan(1 /*child_index*/)->Pds();

	if (CDistributionSpec::EdtReplicated == pdsOuter->Edt() ||
		CDistributionSpec::EdtUniversal == pdsOuter->Edt())
	{
		// if outer is replicated/universal, return inner distribution
		pdsInner->AddRef();
		return pdsInner;
	}

	// otherwise, return outer distribution
	pdsOuter->AddRef();
	return pdsOuter;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalJoin::PrsDerive
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl
	)
	const
{
	CRewindabilitySpec *prsOuter = exprhdl.Pdpplan(0 /*child_index*/)->Prs();
	GPOS_ASSERT(NULL != prsOuter);

	CRewindabilitySpec *prsInner = exprhdl.Pdpplan(1 /*child_index*/)->Prs();
	GPOS_ASSERT(NULL != prsInner);

	CRewindabilitySpec::EMotionHazardType motion_hazard = (prsOuter->HasMotionHazard() || prsInner->HasMotionHazard()) ?
														  CRewindabilitySpec::EmhtMotion :
														  CRewindabilitySpec::EmhtNoMotion;

	if (!prsOuter->IsRewindable() || !prsInner->IsRewindable() || CUtils::FCorrelatedNLJoin(exprhdl.Pop()))
		return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNotRewindable, motion_hazard);

	// else both the children are rewindable
	GPOS_ASSERT(prsOuter->IsRewindable() && prsInner->IsRewindable());
	return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtRewindable, motion_hazard);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalJoin::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	// get rewindability delivered by the join node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();

	if (per->FCompatible(prs))
	{
		// required rewindability will be established by the join operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required rewindability will be enforced on join's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FHashJoinCompatible
//
//	@doc:
//		Are the given predicate parts hash join compatible?
//		predicate parts are obtained by splitting equality into outer
//		and inner expressions
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FHashJoinCompatible
	(
	CExpression *pexprOuter,	// outer child of the join
	CExpression* pexprInner,	// inner child of the join
	CExpression *pexprPredOuter,// outer part of join predicate
	CExpression *pexprPredInner // inner part of join predicate
	)
{
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(NULL != pexprPredOuter);
	GPOS_ASSERT(NULL != pexprPredInner);

	IMDId *pmdidTypeOuter = CScalar::PopConvert(pexprPredOuter->Pop())->MdidType();
	IMDId *pmdidTypeInner = CScalar::PopConvert(pexprPredInner->Pop())->MdidType();

	CColRefSet *pcrsUsedPredOuter = CDrvdPropScalar::GetDrvdScalarProps(pexprPredOuter->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsUsedPredInner = CDrvdPropScalar::GetDrvdScalarProps(pexprPredInner->PdpDerive())->PcrsUsed();

	CColRefSet *outer_refs = CDrvdPropRelational::GetRelationalProperties(pexprOuter->Pdp(DrvdPropArray::EptRelational))->PcrsOutput();
	CColRefSet *pcrsInner = CDrvdPropRelational::GetRelationalProperties(pexprInner->Pdp(DrvdPropArray::EptRelational))->PcrsOutput();

	// make sure that each predicate child uses columns from a different join child
	// in order to reject predicates of the form 'X Join Y on f(X.a, Y.b) = 5'
	BOOL fPredOuterUsesJoinOuterChild = (0 < pcrsUsedPredOuter->Size()) && outer_refs->ContainsAll(pcrsUsedPredOuter);
	BOOL fPredOuterUsesJoinInnerChild = (0 < pcrsUsedPredOuter->Size()) && pcrsInner->ContainsAll(pcrsUsedPredOuter);
	BOOL fPredInnerUsesJoinOuterChild = (0 < pcrsUsedPredInner->Size()) && outer_refs->ContainsAll(pcrsUsedPredInner);
	BOOL fPredInnerUsesJoinInnerChild = (0 < pcrsUsedPredInner->Size()) && pcrsInner->ContainsAll(pcrsUsedPredInner);

	BOOL fHashJoinCompatiblePred =
		(fPredOuterUsesJoinOuterChild && fPredInnerUsesJoinInnerChild) ||
		(fPredOuterUsesJoinInnerChild && fPredInnerUsesJoinOuterChild);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();

	return fHashJoinCompatiblePred &&
					md_accessor->RetrieveType(pmdidTypeOuter)->IsHashable() &&
					md_accessor->RetrieveType(pmdidTypeInner)->IsHashable();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FHashJoinCompatible
//
//	@doc:
//		Is given predicate hash-join compatible?
//		the function returns True if predicate is of the form (Equality) or
//		(Is Not Distinct From), both operands are hashjoinable, AND the predicate
//		uses columns from both join children
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FHashJoinCompatible
	(
	CExpression *pexprPred,		// predicate in question
	CExpression *pexprOuter,	// outer child of the join
	CExpression* pexprInner		// inner child of the join
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(NULL != pexprOuter);
	GPOS_ASSERT(NULL != pexprInner);
	GPOS_ASSERT(pexprOuter != pexprInner);

	BOOL fCompatible = false;
	if (CPredicateUtils::IsEqualityOp(pexprPred) || CPredicateUtils::FINDF(pexprPred))
	{
		CExpression *pexprPredOuter = NULL;
		CExpression *pexprPredInner = NULL;
		ExtractHashJoinExpressions(pexprPred, &pexprPredOuter, &pexprPredInner);
		fCompatible = FHashJoinCompatible(pexprOuter, pexprInner, pexprPredOuter, pexprPredInner);
	}

	return fCompatible;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::ExtractHashJoinExpressions
//
//	@doc:
//		Extract expressions that can be used in hash-join from the given predicate
//
//---------------------------------------------------------------------------
void
CPhysicalJoin::ExtractHashJoinExpressions
	(
	CExpression *pexprPred,
	CExpression **ppexprLeft,
	CExpression **ppexprRight
	)
{
	GPOS_ASSERT(NULL != ppexprLeft);
	GPOS_ASSERT(NULL != ppexprRight);

	*ppexprLeft = NULL;
	*ppexprRight = NULL;

	// extract outer and inner expressions from predicate
	CExpression *pexpr = pexprPred;
	if (!CPredicateUtils::IsEqualityOp(pexprPred))
	{
		GPOS_ASSERT(CPredicateUtils::FINDF(pexpr));
		pexpr = (*pexprPred)[0];
	}

	*ppexprLeft = (*pexpr)[0];
	*ppexprRight = (*pexpr)[1];
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::AddHashKeys
//
//	@doc:
//		Helper for adding a pair of hash join keys to given arrays
//
//---------------------------------------------------------------------------
void
CPhysicalJoin::AddHashKeys
	(
	CExpression *pexprPred,	// equality predicate in the form (ColRef1 = ColRef2) or
							// in the form (ColRef1 is not distinct from ColRef2)
	CExpression *pexprOuter,
	CExpression *
#ifdef GPOS_DEBUG
		pexprInner
#endif // GPOS_DEBUG
	,
	CExpressionArray *pdrgpexprOuter,	// array of outer hash keys
	CExpressionArray *pdrgpexprInner	// array of inner hash keys
	)
{
	GPOS_ASSERT(FHashJoinCompatible(pexprPred, pexprOuter, pexprInner));

	// output of outer side
	CColRefSet *outer_refs = CDrvdPropRelational::GetRelationalProperties(pexprOuter->PdpDerive())->PcrsOutput();

#ifdef GPOS_DEBUG
	// output of inner side
	CColRefSet *pcrsInner = CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsOutput();
#endif // GPOS_DEBUG

	// extract outer and inner columns from predicate
	CExpression *pexprPredOuter = NULL;
	CExpression *pexprPredInner = NULL;
	ExtractHashJoinExpressions(pexprPred, &pexprPredOuter, &pexprPredInner);
	GPOS_ASSERT(NULL != pexprPredOuter);
	GPOS_ASSERT(NULL != pexprPredInner);

	CColRefSet *pcrsPredOuter = CDrvdPropScalar::GetDrvdScalarProps(pexprPredOuter->PdpDerive())->PcrsUsed();
#ifdef GPOS_DEBUG
	CColRefSet *pcrsPredInner = CDrvdPropScalar::GetDrvdScalarProps(pexprPredInner->PdpDerive())->PcrsUsed();
#endif // GPOS_DEBUG

	// determine outer and inner hash keys
	CExpression *pexprKeyOuter = NULL;
	CExpression *pexprKeyInner = NULL;
	if (outer_refs->ContainsAll(pcrsPredOuter))
	{
		pexprKeyOuter = pexprPredOuter;
		GPOS_ASSERT(pcrsInner->ContainsAll(pcrsPredInner));

		pexprKeyInner = pexprPredInner;
	}
	else
	{
		GPOS_ASSERT(outer_refs->ContainsAll(pcrsPredInner));
		pexprKeyOuter = pexprPredInner;

		GPOS_ASSERT(pcrsInner->ContainsAll(pcrsPredOuter));
		pexprKeyInner = pexprPredOuter;
	}
	// remove any BCC as they are no op, else we will have handle them everywhere downstream
	// while comparing two expressions
	CExpression *pexprOuterKeyWithoutBCC = CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprKeyOuter);
	CExpression *pexprInnerKeyWithoutBCC = CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprKeyInner);

	pexprOuterKeyWithoutBCC->AddRef();
	pexprInnerKeyWithoutBCC->AddRef();

	pdrgpexprOuter->Append(pexprOuterKeyWithoutBCC);
	pdrgpexprInner->Append(pexprInnerKeyWithoutBCC);

	GPOS_ASSERT(pdrgpexprInner->Size() == pdrgpexprOuter->Size());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FHashJoinPossible
//
//	@doc:
//		Check if predicate is hashjoin-able and extract arrays of hash keys
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FHashJoinPossible
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	CExpressionArray *pdrgpexprOuter,
	CExpressionArray *pdrgpexprInner,
	CExpression **ppexprResult // output : join expression to be transformed to hash join
	)
{
	GPOS_ASSERT(COperator::EopLogicalNAryJoin != pexpr->Pop()->Eopid() &&
		CUtils::FLogicalJoin(pexpr->Pop()));

	// we should not be here if there are outer references
	GPOS_ASSERT(!CUtils::HasOuterRefs(pexpr));
	GPOS_ASSERT(NULL != ppexprResult);

	// introduce explicit casting, if needed
	CExpressionArray *pdrgpexpr = CCastUtils::PdrgpexprCastEquality(mp,  (*pexpr)[2]);

	// identify hashkeys
	ULONG ulPreds = pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulPreds; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		if (FHashJoinCompatible(pexprPred, (*pexpr)[0], (*pexpr)[1]))
		{
			AddHashKeys(pexprPred, (*pexpr)[0], (*pexpr)[1], pdrgpexprOuter, pdrgpexprInner);
		}
	}
	
	// construct output join expression
	pexpr->Pop()->AddRef();
	(*pexpr)[0]->AddRef();
	(*pexpr)[1]->AddRef();
	*ppexprResult =
		GPOS_NEW(mp) CExpression
			(
			mp,
			pexpr->Pop(),
			(*pexpr)[0],
			(*pexpr)[1],
			CPredicateUtils::PexprConjunction(mp, pdrgpexpr)
			);

	return (0 != pdrgpexprInner->Size());
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::AddFilterOnPartKey
//
//	@doc:
//		 Helper to add filter on part key
//
//---------------------------------------------------------------------------
void
CPhysicalJoin::AddFilterOnPartKey
	(
	IMemoryPool *mp,
	BOOL fNLJoin,
	CExpression *pexprScalar,
	CPartIndexMap *ppimSource,
	CPartFilterMap *ppfmSource,
	ULONG child_index,
	ULONG part_idx_id,
	BOOL fOuterPartConsumer,
	CPartIndexMap *ppimResult,
	CPartFilterMap *ppfmResult,
	CColRefSet *pcrsAllowedRefs
	)
{
	GPOS_ASSERT(NULL != pcrsAllowedRefs);

	ULONG ulChildIndexToTestFirst = 0;
	ULONG ulChildIndexToTestSecond = 1;
	BOOL fOuterPartConsumerTest = fOuterPartConsumer;

	if (fNLJoin)
	{
		ulChildIndexToTestFirst = 1;
		ulChildIndexToTestSecond = 0;
		fOuterPartConsumerTest = !fOuterPartConsumer;
	}

	// look for a filter on the part key
	CExpression *pexprCmp = PexprJoinPredOnPartKeys(mp, pexprScalar, ppimSource, part_idx_id, pcrsAllowedRefs);

	// TODO:  - Aug 14, 2013; create a conjunction of the two predicates when the partition resolver framework supports this
	if (NULL == pexprCmp && ppfmSource->FContainsScanId(part_idx_id))
	{
		// look if a predicates was propagated from an above level
		pexprCmp = ppfmSource->Pexpr(part_idx_id);
		pexprCmp->AddRef();
	}

	if (NULL != pexprCmp)
	{
		if (fOuterPartConsumerTest)
		{
			if (ulChildIndexToTestFirst == child_index)
			{
				// we know that we will be requesting the selector from the second child
				// so we need to increment the number of expected propagators here and pass through
				ppimResult->AddRequiredPartPropagation(ppimSource, part_idx_id, CPartIndexMap::EppraIncrementPropagators);
				pexprCmp->Release();
			}
			else
			{
				// an interesting condition found - request partition selection on the inner child
				ppimResult->AddRequiredPartPropagation(ppimSource, part_idx_id, CPartIndexMap::EppraZeroPropagators);
				ppfmResult->AddPartFilter(mp, part_idx_id, pexprCmp, NULL /*stats*/);
			}
		}
		else
		{
			pexprCmp->Release();
			GPOS_ASSERT(ulChildIndexToTestFirst == child_index);
		}
	}
	else if (FProcessingChildWithPartConsumer(fOuterPartConsumerTest, ulChildIndexToTestFirst, ulChildIndexToTestSecond, child_index))
	{
		// no interesting condition found - push through partition propagation request
		ppimResult->AddRequiredPartPropagation(ppimSource, part_idx_id, CPartIndexMap::EppraPreservePropagators);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FProcessingChildWithPartConsumer
//
//	@doc:
//		Check whether the child being processed is the child that has the part consumer
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FProcessingChildWithPartConsumer
	(
	BOOL fOuterPartConsumerTest,
	ULONG ulChildIndexToTestFirst,
	ULONG ulChildIndexToTestSecond,
	ULONG child_index
	)
{
	return (fOuterPartConsumerTest && ulChildIndexToTestFirst == child_index) ||
			(!fOuterPartConsumerTest && ulChildIndexToTestSecond == child_index);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PexprJoinPredOnPartKeys
//
//	@doc:
//		Helper to find join predicates on part keys. Returns NULL if not found
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalJoin::PexprJoinPredOnPartKeys
	(
	IMemoryPool *mp,
	CExpression *pexprScalar,
	CPartIndexMap *ppimSource,
	ULONG part_idx_id,
	CColRefSet *pcrsAllowedRefs
	)
{
	GPOS_ASSERT(NULL != pcrsAllowedRefs);

	CExpression *pexprPred = NULL;
	CPartKeysArray *pdrgppartkeys = ppimSource->Pdrgppartkeys(part_idx_id);
	const ULONG ulKeysets = pdrgppartkeys->Size();
	for (ULONG ulKey = 0; NULL == pexprPred && ulKey < ulKeysets; ulKey++)
	{
		// get partition key
		CColRef2dArray *pdrgpdrgpcrPartKeys = (*pdrgppartkeys)[ulKey]->Pdrgpdrgpcr();

		// try to generate a request with dynamic partition selection
		pexprPred = CPredicateUtils::PexprExtractPredicatesOnPartKeys
										(
										mp,
										pexprScalar,
										pdrgpdrgpcrPartKeys,
										pcrsAllowedRefs,
										true // fUseConstraints
										);
	}

	return pexprPred;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::FFirstChildToOptimize
//
//	@doc:
//		Helper to check if given child index correspond to first
//		child to be optimized
//
//---------------------------------------------------------------------------
BOOL
CPhysicalJoin::FFirstChildToOptimize
	(
	ULONG child_index
	)
	const
{
	GPOS_ASSERT(2 > child_index);

	EChildExecOrder eceo = Eceo();

	return
		(EceoLeftToRight == eceo && 0 == child_index) ||
		(EceoRightToLeft == eceo && 1 == child_index);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::UlDistrRequestsForCorrelatedJoin
//
//	@doc:
//		Return number of distribution requests for correlated join
//
//---------------------------------------------------------------------------
ULONG
CPhysicalJoin::UlDistrRequestsForCorrelatedJoin()
{
	// Correlated Join creates two distribution requests to enforce distribution of its children:
	// Req(0): If incoming distribution is (Strict) Singleton, pass it through to all children
	//			to comply with correlated execution requirements
	// Req(1): Request outer child for ANY distribution, and then match it on the inner child

	return 2;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PrsRequiredCorrelatedJoin
//
//	@doc:
//		Helper to compute required rewindability of correlated join's children
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalJoin::PrsRequiredCorrelatedJoin
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(3 == exprhdl.Arity());
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(CUtils::FCorrelatedNLJoin(exprhdl.Pop()));

	if (1 == child_index)
	{
		CRewindabilitySpec *prsOuter = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0 /*outer child*/])->Prs();

		CRewindabilitySpec::EMotionHazardType motion_hazard = GPOS_FTRACE(EopttraceMotionHazardHandling) &&
															  (prsOuter->HasMotionHazard() || prsRequired->HasMotionHazard()) ?
															   CRewindabilitySpec::EmhtMotion :
															   CRewindabilitySpec::EmhtNoMotion;

		// if there are outer references, then we need a materialize on inner child
		if (exprhdl.HasOuterRefs())
		{
			return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtRewindable, motion_hazard);
		}
		else
		{
			// if inner child has no outer refs (i.e. subplan with no params) then we need a materialize
			if (!exprhdl.HasOuterRefs(1))
			{
				return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtRewindable, motion_hazard);
			}

			// inner child has no rewindability requirement. if there is something
			// below that needs rewindability (e.g. filter, computescalar, agg), it
			// will be requested where needed. Also we must warn the inner child
			// about motion hazard, such that it can make appropriate rewindability
			// request.
			return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtNotRewindable, motion_hazard);
		}

	}

	GPOS_ASSERT(0 == child_index);

	return PrsRequiredForNLJoinOuterChild(mp, exprhdl, prsRequired);
}

// generate/pass through rewindability request to outer child
CRewindabilitySpec *
CPhysicalJoin::PrsRequiredForNLJoinOuterChild
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired
	)
{
	// if there are outer references, then we need a materialize on outer child
	if (exprhdl.HasOuterRefs())
	{
		return GPOS_NEW(mp) CRewindabilitySpec(CRewindabilitySpec::ErtRewindable, prsRequired->Emht());
	}

	// pass through requirements to outer child
	return PrsPassThru(mp, exprhdl, prsRequired, 0 /*child_index*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::PdsRequiredCorrelatedJoin
//
//	@doc:
//		 Helper to compute required distribution of correlated join's children
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalJoin::PdsRequiredCorrelatedJoin
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG  ulOptReq
	)
	const
{
	GPOS_ASSERT(3 == exprhdl.Arity());
	GPOS_ASSERT(2 > child_index);
	GPOS_ASSERT(CUtils::FCorrelatedNLJoin(exprhdl.Pop()));

	if (0 == ulOptReq && pdsRequired->FSingletonOrStrictSingleton())
	{
		// propagate Singleton request to children to comply with
		// correlated execution requirements
		return PdsPassThru(mp, exprhdl, pdsRequired, child_index);
	}

	if (exprhdl.PfpChild(1)->FHasVolatileFunctionScan() && exprhdl.HasOuterRefs(1))
	{
		// if the inner child has a volatile TVF and has outer refs then request
		// gather from both children
		return GPOS_NEW(mp) CDistributionSpecSingleton();
	}

	if (1 == child_index)
	{
		CDistributionSpec *pdsOuter = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds();
		if (CDistributionSpec::EdtUniversal == pdsOuter->Edt())
		{
			// if outer child delivers a universal distribution, request inner child
			// to match Singleton distribution to detect more than one row generated
			// at runtime, for example: 'select (select 1 union select 2)'
			return GPOS_NEW(mp) CDistributionSpecSingleton();
		}
	}

	return CPhysicalJoin::PdsRequired(mp, exprhdl, pdsRequired, child_index, pdrgpdpCtxt, ulOptReq);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalJoin::Edm
//
//	@doc:
//		 Distribution matching type
//
//---------------------------------------------------------------------------
CEnfdDistribution::EDistributionMatching
CPhysicalJoin::Edm
	(
	CReqdPropPlan *, // prppInput
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG // ulOptReq
	)
{
	if (FFirstChildToOptimize(child_index))
	{
		// use satisfaction for the first child to be optimized
		return CEnfdDistribution::EdmSatisfy;
	}

	// extract distribution type of previously optimized child
	GPOS_ASSERT(NULL != pdrgpdpCtxt);
	CDistributionSpec::EDistributionType edtPrevChild = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0])->Pds()->Edt();

	if (CDistributionSpec::EdtReplicated == edtPrevChild ||
	    CDistributionSpec::EdtUniversal == edtPrevChild)
	{
		// if previous child is replicated or universal, we use
		// distribution satisfaction for current child
		return CEnfdDistribution::EdmSatisfy;
	}

	// use exact matching for all other children
	return CEnfdDistribution::EdmExact;
}


// Hash function
ULONG
CPhysicalJoin::CPartPropReq::HashValue
	(
	const CPartPropReq *pppr
	)
{
	GPOS_ASSERT(NULL != pppr);

	ULONG ulHash = pppr->Ppps()->HashValue();
	ulHash = CombineHashes(ulHash , pppr->UlChildIndex());
	ulHash = CombineHashes(ulHash , pppr->UlOuterChild());
	ulHash = CombineHashes(ulHash , pppr->UlInnerChild());

	return CombineHashes(ulHash , pppr->UlScalarChild());
}

// Equality function
BOOL
CPhysicalJoin::CPartPropReq::Equals
	(
	const CPartPropReq *ppprFst,
	const CPartPropReq *ppprSnd
	)
{
	GPOS_ASSERT(NULL != ppprFst);
	GPOS_ASSERT(NULL != ppprSnd);

	return
		ppprFst->UlChildIndex() == ppprSnd->UlChildIndex() &&
		ppprFst->UlOuterChild() == ppprSnd->UlOuterChild() &&
		ppprFst->UlInnerChild() == ppprSnd->UlInnerChild() &&
		ppprFst->UlScalarChild() == ppprSnd->UlScalarChild() &&
		ppprFst->Ppps()->Matches(ppprSnd->Ppps());
}


// Create partition propagation request
CPhysicalJoin::CPartPropReq *
CPhysicalJoin::PpprCreate
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG child_index
	)
{
	GPOS_ASSERT(exprhdl.Pop() == this);
	GPOS_ASSERT(NULL != pppsRequired);
	if (NULL == exprhdl.Pgexpr())
	{
		return NULL;
	}

	ULONG ulOuterChild = (*exprhdl.Pgexpr())[0]->Id();
	ULONG ulInnerChild = (*exprhdl.Pgexpr())[1]->Id();
	ULONG ulScalarChild = (*exprhdl.Pgexpr())[2]->Id();

	pppsRequired->AddRef();
	return  GPOS_NEW(mp) CPartPropReq(pppsRequired, child_index, ulOuterChild, ulInnerChild, ulScalarChild);
}


// Compute required partition propagation of the n-th child
CPartitionPropagationSpec *
CPhysicalJoin::PppsRequiredCompute
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG child_index,
	BOOL fNLJoin
	)
{
	CPartIndexMap *ppim = pppsRequired->Ppim();
	CPartFilterMap *ppfm = pppsRequired->Ppfm();

	ULongPtrArray *pdrgpul = ppim->PdrgpulScanIds(mp);

	CPartIndexMap *ppimResult = GPOS_NEW(mp) CPartIndexMap(mp);
	CPartFilterMap *ppfmResult = GPOS_NEW(mp) CPartFilterMap(mp);

	// get outer partition consumers
	CPartInfo *ppartinfo = exprhdl.GetRelationalProperties(0)->Ppartinfo();

	CColRefSet *pcrsOutputOuter = exprhdl.GetRelationalProperties(0)->PcrsOutput();
	CColRefSet *pcrsOutputInner = exprhdl.GetRelationalProperties(1)->PcrsOutput();

	const ULONG ulPartIndexIds = pdrgpul->Size();

	for (ULONG ul = 0; ul < ulPartIndexIds; ul++)
	{
		ULONG part_idx_id = *((*pdrgpul)[ul]);

		if (ppfm->FContainsScanId(part_idx_id))
		{
			GPOS_ASSERT(NULL != ppfm->Pexpr(part_idx_id));
			// a selection-based propagation request pushed from above: do not propagate any
			// further as the join will reduce cardinality and thus may select more partitions
			// for scanning
			continue;
		}

		BOOL fOuterPartConsumer = ppartinfo->FContainsScanId(part_idx_id);

		// in order to find interesting join predicates that can be used for DPE,
		// one side of the predicate must be the partition key, while the other side must only contain
		// references from the join child that does not have the partition consumer
		CColRefSet *pcrsAllowedRefs = pcrsOutputOuter;
		if (fOuterPartConsumer)
		{
			pcrsAllowedRefs = pcrsOutputInner;
		}

		if(fNLJoin)
		{
			if (0 == child_index && fOuterPartConsumer)
			{
				// always push through required partition propagation for consumers on the
				// outer side of the nested loop join
				CPartKeysArray *pdrgppartkeys = ppartinfo->PdrgppartkeysByScanId(part_idx_id);
				GPOS_ASSERT(NULL != pdrgppartkeys);
				pdrgppartkeys->AddRef();

				ppimResult->AddRequiredPartPropagation(ppim, part_idx_id, CPartIndexMap::EppraPreservePropagators, pdrgppartkeys);
			}
			else
			{
				// check if there is an interesting condition involving the partition key
				CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*child_index*/);
				AddFilterOnPartKey(mp, true /*fNLJoin*/, pexprScalar, ppim, ppfm, child_index, part_idx_id, fOuterPartConsumer, ppimResult, ppfmResult, pcrsAllowedRefs);
			}
		}
		else
		{
			if (1 == child_index && !fOuterPartConsumer)
			{
				// always push through required partition propagation for consumers on the
				// inner side of the hash join
				CPartKeysArray *pdrgppartkeys = exprhdl.GetRelationalProperties(1 /*child_index*/)->Ppartinfo()->PdrgppartkeysByScanId(part_idx_id);
				GPOS_ASSERT(NULL != pdrgppartkeys);
				pdrgppartkeys->AddRef();

				ppimResult->AddRequiredPartPropagation(ppim, part_idx_id, CPartIndexMap::EppraPreservePropagators, pdrgppartkeys);
			}
			else
			{
				// look for a filter on the part key
				CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*child_index*/);
				AddFilterOnPartKey(mp, false /*fNLJoin*/, pexprScalar, ppim, ppfm, child_index, part_idx_id, fOuterPartConsumer, ppimResult, ppfmResult, pcrsAllowedRefs);
			}
		}
	}

	pdrgpul->Release();

	return GPOS_NEW(mp) CPartitionPropagationSpec(ppimResult, ppfmResult);
}

// Compute required partition propagation of the n-th child
CPartitionPropagationSpec *
CPhysicalJoin::PppsRequiredJoinChild
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG child_index,
	CDrvdProp2dArray *, //pdrgpdpCtxt,
	BOOL fNLJoin
	)
{
	GPOS_ASSERT(NULL != pppsRequired);

	CPartPropReq *pppr = PpprCreate(mp, exprhdl, pppsRequired, child_index);
	if (NULL == pppr)
	{
		return PppsRequiredCompute(mp, exprhdl, pppsRequired, child_index, fNLJoin);
	}

	CAutoMutex am(m_mutexJoin);
	am.Lock();

	CPartitionPropagationSpec *ppps = m_phmpp->Find(pppr);
	if (NULL == ppps)
	{
		ppps = PppsRequiredCompute(mp, exprhdl, pppsRequired, child_index, fNLJoin);
#ifdef GPOS_DEBUG
		BOOL fSuccess =
#endif // GPOS_DEBUG
			m_phmpp->Insert(pppr, ppps);
		GPOS_ASSERT(fSuccess);
	}
	else
	{
		pppr->Release();
	}

	ppps->AddRef();
	return ppps;
}

// EOF
