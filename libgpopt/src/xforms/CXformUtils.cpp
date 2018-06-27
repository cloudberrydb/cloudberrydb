//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CXformUtils.cpp
//
//	@doc:
//		Implementation of xform utilities
//---------------------------------------------------------------------------


#include "gpos/base.h"
#include "gpos/error/CMessage.h"
#include "gpos/error/CMessageRepository.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDTriggerGPDB.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/md/IMDTrigger.h"
#include "naucrates/md/IMDCheckConstraint.h"

#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/CConstraintNegation.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/search/CGroupProxy.h"
#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/xforms/CDecorrelator.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/exception.h"
#include "gpopt/engine/CHint.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ExfpLogicalJoin2PhysicalJoin
//
//	@doc:
//		Check the applicability of logical join to physical join xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformUtils::ExfpLogicalJoin2PhysicalJoin
	(
	CExpressionHandle &exprhdl
	)
{
	// if scalar predicate has a subquery, we must have an
	// equivalent logical Apply expression created during exploration;
	// no need for generating a physical join
	if (exprhdl.Pdpscalar(2)->FHasSubquery())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ExfpSemiJoin2CrossProduct
//
//	@doc:
//		Check the applicability of semi join to cross product xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformUtils::ExfpSemiJoin2CrossProduct
	(
	CExpressionHandle &exprhdl
	)
{
#ifdef GPOS_DEBUG
	COperator::EOperatorId eopid =  exprhdl.Pop()->Eopid();
#endif // GPOS_DEBUG
	GPOS_ASSERT(COperator::EopLogicalLeftSemiJoin == eopid ||
			COperator::EopLogicalLeftAntiSemiJoin == eopid ||
			COperator::EopLogicalLeftAntiSemiJoinNotIn == eopid);

	CColRefSet *pcrsUsed = exprhdl.Pdpscalar(2 /*ulChildIndex*/)->PcrsUsed();
	CColRefSet *pcrsOuterOutput = exprhdl.Pdprel(0 /*ulChildIndex*/)->PcrsOutput();
	if (0 == pcrsUsed->CElements() || !pcrsOuterOutput->FSubset(pcrsUsed))
	{
		// xform is inapplicable of join predicate uses columns from join's inner child
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ExfpExpandJoinOrder
//
//	@doc:
//		Check the applicability of N-ary join expansion
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformUtils::ExfpExpandJoinOrder
	(
	CExpressionHandle &exprhdl
	)
{
	if (exprhdl.Pdpscalar(exprhdl.UlArity() - 1)->FHasSubquery() || exprhdl.FHasOuterRefs())
	{
		// subqueries must be unnested before applying xform
		return CXform::ExfpNone;
	}

#ifdef GPOS_DEBUG
	CAutoMemoryPool amp;
	GPOS_ASSERT(!FJoinPredOnSingleChild(amp.Pmp(), exprhdl) &&
			"join predicates are not pushed down");
#endif // GPOS_DEBUG

	if (NULL != exprhdl.Pgexpr())
	{
		// if handle is attached to a group expression, transformation is applied
		// to the Memo and we need to check if stats are derivable on child groups
		CGroup *pgroup = exprhdl.Pgexpr()->Pgroup();
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();
		if (!pgroup->FStatsDerivable(pmp))
		{
			// stats must be derivable before applying xforms
			return CXform::ExfpNone;
		}

		const ULONG ulArity = exprhdl.UlArity();
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CGroup *pgroupChild = (*exprhdl.Pgexpr())[ul];
			if (!pgroupChild->FScalar() && !pgroupChild->FStatsDerivable(pmp))
			{
				// stats must be derivable on every child
				return CXform::ExfpNone;
			}
		}
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FInlinableCTE
//
//	@doc:
//		Check whether a CTE should be inlined
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FInlinableCTE
	(
	ULONG ulCTEId
	)
{
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	CExpression *pexprProducer = pcteinfo->PexprCTEProducer(ulCTEId);
	GPOS_ASSERT(NULL != pexprProducer);
	CFunctionProp *pfp = CDrvdPropRelational::Pdprel(pexprProducer->PdpDerive())->Pfp();

	CPartInfo *ppartinfoCTEProducer = CDrvdPropRelational::Pdprel(pexprProducer->PdpDerive())->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfoCTEProducer);
	
	return IMDFunction::EfsVolatile > pfp->Efs() && 
			!pfp->FMasterOnly() &&
			(0 == ppartinfoCTEProducer->UlConsumers() || 1 == pcteinfo->UlConsumers(ulCTEId));
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PcrsFKey
//
//	@doc:
//		Helper to construct a foreign key by collecting columns that appear
//		in equality predicates with primary key columns;
//		return NULL if no foreign key could be constructed
//
//---------------------------------------------------------------------------
CColRefSet *
CXformUtils::PcrsFKey
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr, // array of scalar conjuncts
	CColRefSet *prcsOutput, // output columns of outer expression
	CColRefSet *pcrsKey // a primary key of a inner expression
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pcrsKey);
	GPOS_ASSERT(NULL != prcsOutput);

	 // collected columns that are part of primary key and used in equality predicates
	CColRefSet *pcrsKeyParts = GPOS_NEW(pmp) CColRefSet(pmp);

	// FK columns
	CColRefSet *pcrsFKey = GPOS_NEW(pmp) CColRefSet(pmp);
	const ULONG ulConjuncts = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulConjuncts; ul++)
	{
		CExpression *pexprConjunct = (*pdrgpexpr)[ul];
		if (!CPredicateUtils::FPlainEquality(pexprConjunct))
		{
			continue;
		}

		CColRef *pcrFst = const_cast<CColRef*> (CScalarIdent::PopConvert((*pexprConjunct)[0]->Pop())->Pcr());
		CColRef *pcrSnd = const_cast<CColRef*> (CScalarIdent::PopConvert((*pexprConjunct)[1]->Pop())->Pcr());
		if (pcrsKey->FMember(pcrFst) && prcsOutput->FMember(pcrSnd))
		{
			pcrsKeyParts->Include(pcrFst);
			pcrsFKey->Include(pcrSnd);
		}
		else if (pcrsKey->FMember(pcrSnd) && prcsOutput->FMember(pcrFst))
		{
			pcrsFKey->Include(pcrFst);
			pcrsKeyParts->Include(pcrSnd);
		}
	}

	// check if collected key parts constitute a primary key
	if (!pcrsKeyParts->FEqual(pcrsKey))
	{
		// did not succeeded in building foreign key
		pcrsFKey->Release();
		pcrsFKey = NULL;
	}
	pcrsKeyParts->Release();

	return pcrsFKey;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PcrsFKey
//
//	@doc:
//		Return a foreign key pointing from outer expression to inner
//		expression;
//		return NULL if no foreign key could be extracted
//
//---------------------------------------------------------------------------
CColRefSet *
CXformUtils::PcrsFKey
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter,
	CExpression *pexprInner,
	CExpression *pexprScalar
	)
{
	// get inner expression key
	CKeyCollection *pkc =  CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->Pkc();
	if (NULL == pkc)
	{
		// inner expression has no key
		return NULL;
	}
	// get outer expression output columns
	CColRefSet *prcsOutput = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();

	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	CColRefSet *pcrsFKey = NULL;

	const ULONG ulKeys = pkc->UlKeys();
	for (ULONG ulKey = 0; ulKey < ulKeys; ulKey++)
	{
		DrgPcr *pdrgpcrKey = pkc->PdrgpcrKey(pmp, ulKey);

		CColRefSet *pcrsKey = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsKey->Include(pdrgpcrKey);
		pdrgpcrKey->Release();

		// attempt to construct a foreign key based on current primary key
		pcrsFKey = PcrsFKey(pmp, pdrgpexpr, prcsOutput, pcrsKey);
		pcrsKey->Release();

		if (NULL != pcrsFKey)
		{
			// succeeded in finding FK
			break;
		}
	}

	pdrgpexpr->Release();

	return pcrsFKey;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprRedundantSelectForDynamicIndex
//
//	@doc:
// 		Add a redundant SELECT node on top of Dynamic (Bitmap) IndexGet to
//		to be able to use index predicate in partition elimination
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprRedundantSelectForDynamicIndex
	(
	IMemoryPool *pmp,
	CExpression *pexpr // input expression is a dynamic (bitmap) IndexGet with an optional Select on top
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopLogicalDynamicIndexGet == eopid ||
			COperator::EopLogicalDynamicBitmapTableGet == eopid ||
			COperator::EopLogicalSelect == eopid);

	CExpression *pexprRedundantScalar = NULL;
	if (COperator::EopLogicalDynamicIndexGet == eopid || COperator::EopLogicalDynamicBitmapTableGet == eopid)
	{
		// no residual predicate, use index lookup predicate only
		pexpr->AddRef();
		// reuse index lookup predicate
		(*pexpr)[0]->AddRef();
		pexprRedundantScalar = (*pexpr)[0];

		return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalSelect(pmp), pexpr, pexprRedundantScalar);
	}

	// there is a residual predicate in a SELECT node on top of DynamicIndexGet,
	// we create a conjunction of both residual predicate and index lookup predicate
	CExpression *pexprChild = (*pexpr)[0];
#ifdef GPOS_DEBUG
	COperator::EOperatorId eopidChild = pexprChild->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopLogicalDynamicIndexGet == eopidChild ||
				COperator::EopLogicalDynamicBitmapTableGet == eopidChild);
#endif // GPOS_DEBUG

	CExpression *pexprIndexLookupPred = (*pexprChild)[0];
	CExpression *pexprResidualPred = (*pexpr)[1];
	pexprRedundantScalar = CPredicateUtils::PexprConjunction(pmp, pexprIndexLookupPred, pexprResidualPred);

	pexprChild->AddRef();

	return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalSelect(pmp),
				pexprChild,
				pexprRedundantScalar
				);
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FSwapableJoinType
//
//	@doc:
//		Check whether the given join type is swapable
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FSwapableJoinType
	(
	COperator::EOperatorId eopid
	)
{
	return (COperator::EopLogicalLeftSemiJoin == eopid ||
			COperator::EopLogicalLeftAntiSemiJoin == eopid ||
			COperator::EopLogicalLeftAntiSemiJoinNotIn == eopid ||
			COperator::EopLogicalInnerJoin == eopid);
}
#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprSwapJoins
//
//	@doc:
//		Compute a swap of the two given joins;
//		the function returns null if swapping cannot be done
//
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprSwapJoins
	(
	IMemoryPool *pmp,
	CExpression *pexprTopJoin,
	CExpression *pexprBottomJoin
	)
{
#ifdef GPOS_DEBUG
	COperator::EOperatorId eopidTop = pexprTopJoin->Pop()->Eopid();
	COperator::EOperatorId eopidBottom = pexprBottomJoin->Pop()->Eopid();
#endif // GPOS_DEBUG

	GPOS_ASSERT(FSwapableJoinType(eopidTop) && FSwapableJoinType(eopidBottom));
	GPOS_ASSERT_IMP(COperator::EopLogicalInnerJoin == eopidTop,
			COperator::EopLogicalLeftSemiJoin == eopidBottom ||
			COperator::EopLogicalLeftAntiSemiJoin == eopidBottom ||
			COperator::EopLogicalLeftAntiSemiJoinNotIn == eopidBottom);

	// get used columns by the join predicate of top join
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar((*pexprTopJoin)[2]->PdpDerive())->PcrsUsed();

	// get output columns of bottom join's children
	const CColRefSet *pcrsBottomOuter = CDrvdPropRelational::Pdprel((*pexprBottomJoin)[0]->PdpDerive())->PcrsOutput();
	const CColRefSet *pcrsBottomInner = CDrvdPropRelational::Pdprel((*pexprBottomJoin)[1]->PdpDerive())->PcrsOutput();

	BOOL fDisjointWithBottomOuter = pcrsUsed->FDisjoint(pcrsBottomOuter);
	BOOL fDisjointWithBottomInner = pcrsUsed->FDisjoint(pcrsBottomInner);
	if (!fDisjointWithBottomOuter && !fDisjointWithBottomInner)
	{
		// top join uses columns from both children of bottom join;
		// join swap is not possible
		return NULL;
	}

	CExpression *pexprChild = (*pexprBottomJoin)[0];
	CExpression *pexprChildOther = (*pexprBottomJoin)[1];
	if (fDisjointWithBottomOuter && !fDisjointWithBottomInner)
	{
		pexprChild = (*pexprBottomJoin)[1];
		pexprChildOther = (*pexprBottomJoin)[0];
	}

	CExpression *pexprRight = (*pexprTopJoin)[1];
	CExpression *pexprScalar = (*pexprTopJoin)[2];
	COperator *pop = pexprTopJoin->Pop();
	pop->AddRef();
	pexprChild->AddRef();
	pexprRight->AddRef();
	pexprScalar->AddRef();
	CExpression *pexprNewBottomJoin = GPOS_NEW(pmp) CExpression
								(
								pmp,
								pop,
								pexprChild,
								pexprRight,
								pexprScalar
								);

	pexprScalar = (*pexprBottomJoin)[2];
	pop = pexprBottomJoin->Pop();
	pop->AddRef();
	pexprChildOther->AddRef();
	pexprScalar->AddRef();

	// return a new expression with the two joins swapped
	return  GPOS_NEW(pmp) CExpression
						(
						pmp,
						pop,
						pexprNewBottomJoin,
						pexprChildOther,
						pexprScalar
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprPushGbBelowJoin
//
//	@doc:
//		Push a Gb, optionally with a having clause below the child join;
//		if push down fails, the function returns NULL expression
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprPushGbBelowJoin
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();

	GPOS_ASSERT(COperator::EopLogicalGbAgg == eopid ||
				COperator::EopLogicalGbAggDeduplicate == eopid ||
				COperator::EopLogicalSelect == eopid);

	CExpression *pexprSelect = NULL;
	CExpression *pexprGb = pexpr;
	if (COperator::EopLogicalSelect == eopid)
	{
		pexprSelect = pexpr;
		pexprGb = (*pexpr)[0];
	}

	CExpression *pexprJoin = (*pexprGb)[0];
	CExpression *pexprPrjList = (*pexprGb)[1];
	CExpression *pexprOuter = (*pexprJoin)[0];
	CExpression *pexprInner = (*pexprJoin)[1];
	CExpression *pexprScalar = (*pexprJoin)[2];
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGb->Pop());

	CColRefSet *pcrsOuterOutput = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsAggOutput = CDrvdPropRelational::Pdprel(pexprGb->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprPrjList->PdpDerive())->PcrsUsed();
	CColRefSet *pcrsFKey = PcrsFKey(pmp, pexprOuter, pexprInner, pexprScalar);
	
	CColRefSet *pcrsScalarFromOuter = GPOS_NEW(pmp) CColRefSet(pmp, *(CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed()));
	pcrsScalarFromOuter->Intersection(pcrsOuterOutput);
	
	// use minimal grouping columns if they exist, otherwise use all grouping columns
	CColRefSet *pcrsGrpCols = GPOS_NEW(pmp) CColRefSet(pmp);
	DrgPcr *pdrgpcr = popGbAgg->PdrgpcrMinimal();
	if (NULL == pdrgpcr)
	{
		pdrgpcr = popGbAgg->Pdrgpcr();
	}
	pcrsGrpCols->Include(pdrgpcr);

	BOOL fCanPush = FCanPushGbAggBelowJoin(pcrsGrpCols, pcrsOuterOutput, pcrsScalarFromOuter, pcrsAggOutput, pcrsUsed, pcrsFKey);
	
	// cleanup
	CRefCount::SafeRelease(pcrsFKey);
	pcrsScalarFromOuter->Release();

	if (!fCanPush)
	{
		pcrsGrpCols->Release();

		return NULL;
	}

	// here, we know that grouping columns include FK and all used columns by Gb
	// come only from the outer child of the join;
	// we can safely push Gb to be on top of join's outer child

	popGbAgg->AddRef();
	CLogicalGbAgg *popGbAggNew = PopGbAggPushableBelowJoin(pmp, popGbAgg, pcrsOuterOutput, pcrsGrpCols);
	pcrsGrpCols->Release();

	pexprOuter->AddRef();
	pexprPrjList->AddRef();
	CExpression *pexprNewGb = GPOS_NEW(pmp) CExpression(pmp, popGbAggNew, pexprOuter, pexprPrjList);

	CExpression *pexprNewOuter = pexprNewGb;
	if (NULL != pexprSelect)
	{
		// add Select node on top of Gb
		(*pexprSelect)[1]->AddRef();
		pexprNewOuter = CUtils::PexprLogicalSelect(pmp, pexprNewGb, (*pexprSelect)[1]);
	}

	COperator *popJoin = pexprJoin->Pop();
	popJoin->AddRef();
	pexprInner->AddRef();
	pexprScalar->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, popJoin, pexprNewOuter, pexprInner, pexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PopGbAggPushableBelowJoin
//
//	@doc:
//		Create the Gb operator to be pushed below a join given the original Gb
//		operator, output columns of the join's outer child and grouping cols
//
//---------------------------------------------------------------------------
CLogicalGbAgg *
CXformUtils::PopGbAggPushableBelowJoin
	(
	IMemoryPool *pmp,
	CLogicalGbAgg *popGbAggOld,
	CColRefSet *pcrsOutputOuter,
	CColRefSet *pcrsGrpCols
	)
{
	GPOS_ASSERT(NULL != popGbAggOld);
	GPOS_ASSERT(NULL != pcrsOutputOuter);
	GPOS_ASSERT(NULL != pcrsGrpCols);

	CLogicalGbAgg *popGbAggNew = popGbAggOld;
	if (!pcrsOutputOuter->FSubset(pcrsGrpCols))
	{
		// we have grouping columns from both join children;
		// we can drop grouping columns from the inner join child since
		// we already have a FK in grouping columns

		popGbAggNew->Release();
		CColRefSet *pcrsGrpColsNew = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsGrpColsNew->Include(pcrsGrpCols);
		pcrsGrpColsNew->Intersection(pcrsOutputOuter);
		if (COperator::EopLogicalGbAggDeduplicate == popGbAggOld->Eopid())
		{
			DrgPcr *pdrgpcrKeys = CLogicalGbAggDeduplicate::PopConvert(popGbAggOld)->PdrgpcrKeys();
			pdrgpcrKeys->AddRef();
			popGbAggNew = GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp, pcrsGrpColsNew->Pdrgpcr(pmp), popGbAggOld->Egbaggtype(), pdrgpcrKeys);
		}
		else
		{
			popGbAggNew = GPOS_NEW(pmp) CLogicalGbAgg(pmp, pcrsGrpColsNew->Pdrgpcr(pmp), popGbAggOld->Egbaggtype());
		}
		pcrsGrpColsNew->Release();
	}

	return popGbAggNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FCanPushGbAggBelowJoin
//
//	@doc:
//		Check if the preconditions for pushing down Group by through join are 
//		satisfied
//---------------------------------------------------------------------------
BOOL
CXformUtils::FCanPushGbAggBelowJoin
	(
	CColRefSet *pcrsGrpCols,
	CColRefSet *pcrsJoinOuterChildOutput,
	CColRefSet *pcrsJoinScalarUsedFromOuter,
	CColRefSet *pcrsGrpByOutput,
	CColRefSet *pcrsGrpByUsed,
	CColRefSet *pcrsFKey
	)
{
	BOOL fGrpByProvidesUsedColumns = pcrsGrpByOutput->FSubset(pcrsJoinScalarUsedFromOuter);

	BOOL fHasFK = (NULL != pcrsFKey);
	BOOL fGrpColsContainFK = (fHasFK && pcrsGrpCols->FSubset(pcrsFKey));
	BOOL fOutputColsContainUsedCols = pcrsJoinOuterChildOutput->FSubset(pcrsGrpByUsed);

	if (!fHasFK || !fGrpColsContainFK || !fOutputColsContainUsedCols || !fGrpByProvidesUsedColumns)
	{
		// GrpBy cannot be pushed through join because
		// (1) no FK exists, or
		// (2) FK exists but grouping columns do not include it, or
		// (3) Gb uses columns from both join children, or
		// (4) Gb hides columns required for the join scalar child

		return false;
	}
	
	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FSameDatatype
//
//	@doc:
//		Check if the input columns of the outer child are the same as the
//		aligned input columns of the each of the inner children.
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FSameDatatype
	(
	DrgDrgPcr *pdrgpdrgpcrInput
	)
{
	GPOS_ASSERT(1 < pdrgpdrgpcrInput->UlLength());

	DrgPcr *pdrgpcrOuter = (*pdrgpdrgpcrInput)[0];

	ULONG ulColIndex = pdrgpcrOuter->UlLength();
	ULONG ulChildIndex = pdrgpdrgpcrInput->UlLength();
	for (ULONG ulColCounter = 0; ulColCounter < ulColIndex; ulColCounter++)
	{
		CColRef *pcrOuter = (*pdrgpcrOuter)[ulColCounter];

		for (ULONG ulChildCounter = 1; ulChildCounter < ulChildIndex; ulChildCounter++)
		{
			DrgPcr *pdrgpcrInnner = (*pdrgpdrgpcrInput)[ulChildCounter];
			CColRef *pcrInner = (*pdrgpcrInnner)[ulColCounter];

			GPOS_ASSERT(pcrOuter != pcrInner);

			if (pcrInner->Pmdtype() != pcrOuter->Pmdtype())
			{
				return false;
			}
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ExistentialToAgg
//
//	@doc:
//		Helper for creating Agg expression equivalent to an existential subquery
//
//		Example:
//			For 'exists(select * from r where a = 10)', we produce the following:
//			New Subquery: (select count(*) as cc from r where a = 10)
//			New Scalar: cc > 0
//
//---------------------------------------------------------------------------
void
CXformUtils::ExistentialToAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprSubquery,
	CExpression **ppexprNewSubquery, // output argument for new scalar subquery
	CExpression **ppexprNewScalar   // output argument for new scalar expression
	)
{
	GPOS_ASSERT(CUtils::FExistentialSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(NULL != ppexprNewSubquery);
	GPOS_ASSERT(NULL != ppexprNewScalar);

	COperator::EOperatorId eopid = pexprSubquery->Pop()->Eopid();
	CExpression *pexprInner = (*pexprSubquery)[0];
	IMDType::ECmpType ecmptype = IMDType::EcmptG;
	if (COperator::EopScalarSubqueryNotExists == eopid)
	{
		ecmptype = IMDType::EcmptEq;
	}

	pexprInner->AddRef();
	CExpression *pexprInnerNew = CUtils::PexprCountStar(pmp, pexprInner);
	const CColRef *pcrCount = CScalarProjectElement::PopConvert((*(*pexprInnerNew)[1])[0]->Pop())->Pcr();

	*ppexprNewSubquery = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrCount, true /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/), pexprInnerNew);
	*ppexprNewScalar =
			CUtils::PexprCmpWithZero
			(
			pmp,
			CUtils::PexprScalarIdent(pmp, pcrCount),
			pcrCount->Pmdtype()->Pmdid(),
			ecmptype
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::QuantifiedToAgg
//
//	@doc:
//		Helper for creating Agg expression equivalent to a quantified subquery,
//
//
//---------------------------------------------------------------------------
void
CXformUtils::QuantifiedToAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprSubquery,
	CExpression **ppexprNewSubquery, // output argument for new scalar subquery
	CExpression **ppexprNewScalar   // output argument for new scalar expression
	)
{
	GPOS_ASSERT(CUtils::FQuantifiedSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(NULL != ppexprNewSubquery);
	GPOS_ASSERT(NULL != ppexprNewScalar);

	if (COperator::EopScalarSubqueryAll == pexprSubquery->Pop()->Eopid())
	{
		return SubqueryAllToAgg(pmp, pexprSubquery, ppexprNewSubquery, ppexprNewScalar);
	}

	return SubqueryAnyToAgg(pmp, pexprSubquery, ppexprNewSubquery, ppexprNewScalar);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::SubqueryAnyToAgg
//
//	@doc:
//		Helper for transforming SubqueryAll into aggregate subquery,
//		we need to differentiate between two cases:
//		(1) if subquery predicate uses nullable columns, we may produce null values,
//			this is handeled by adding a null indicator to subquery column, and
//			counting the number of subquery results with null value in subquery column,
//		(2) if subquery predicate does not use nullable columns, we can only produce
//			boolean values,
//
//		Examples:
//
//			- For 'b1 in (select b2 from R)', with b2 not-nullable, we produce the following:
//			* New Subquery: (select count(*) as cc from R where b1=b2))
//			* New Scalar: cc > 0
//
//			- For 'b1 in (select b2 from R)', with b2 nullable, we produce the following:
//			* New Subquery: (select Prj_cc from (select count(*), sum(null_indic) from R where b1=b2))
//			where Prj_cc is a project for column cc, defined as follows:
//				if (count(*) == 0), then cc = 0,
//				else if (count(*) == sum(null_indic)), then cc = -1,
//				else cc = count(*)
//			where (-1) indicates that subquery produced a null (this is replaced by NULL in
//			SubqueryHandler when unnesting to LeftApply)
//			* New Scalar: cc > 0
//
//
//---------------------------------------------------------------------------
void
CXformUtils::SubqueryAnyToAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprSubquery,
	CExpression **ppexprNewSubquery, // output argument for new scalar subquery
	CExpression **ppexprNewScalar   // output argument for new scalar expression
	)
{
	GPOS_ASSERT(CUtils::FQuantifiedSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == pexprSubquery->Pop()->Eopid());
	GPOS_ASSERT(NULL != ppexprNewSubquery);
	GPOS_ASSERT(NULL != ppexprNewScalar);

	CExpression *pexprInner = (*pexprSubquery)[0];

	// build subquery quantified comparison
	CExpression *pexprResult = NULL;
	CSubqueryHandler sh(pmp, false /* fEnforceCorrelatedApply */);
	CExpression *pexprSubqPred = sh.PexprSubqueryPred(pexprInner, pexprSubquery, &pexprResult);

	const CColRef *pcrSubq = CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop())->Pcr();
	BOOL fUsesNullableCol = CUtils::FUsesNullableCol(pmp, pexprSubqPred, pexprResult);

	CExpression *pexprInnerNew = NULL;
	pexprInner->AddRef();
	if (fUsesNullableCol)
	{
		// add a null indicator
		CExpression *pexprNullIndicator = PexprNullIndicator(pmp, CUtils::PexprScalarIdent(pmp, pcrSubq));
		CExpression *pexprPrj = CUtils::PexprAddProjection(pmp, pexprResult, pexprNullIndicator);
		pexprResult = pexprPrj;

		// add disjunction with is not null check
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		pdrgpexpr->Append(pexprSubqPred);
		pdrgpexpr->Append(CUtils::PexprIsNull(pmp, CUtils::PexprScalarIdent(pmp, pcrSubq)));

		pexprSubqPred = CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr);
	}

	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprResult, pexprSubqPred);
	if (fUsesNullableCol)
	{
		const CColRef *pcrNullIndicator = CScalarProjectElement::PopConvert((*(*(*pexprSelect)[0])[1])[0]->Pop())->Pcr();
		pexprInnerNew = CUtils::PexprCountStarAndSum(pmp, pcrNullIndicator, pexprSelect);
		const CColRef *pcrCount = CScalarProjectElement::PopConvert((*(*pexprInnerNew)[1])[0]->Pop())->Pcr();
		const CColRef *pcrSum = CScalarProjectElement::PopConvert((*(*pexprInnerNew)[1])[1]->Pop())->Pcr();

		CExpression *pexprScalarIdentCount = CUtils::PexprScalarIdent(pmp, pcrCount);
		CExpression *pexprCountEqZero = CUtils::PexprCmpWithZero(pmp, pexprScalarIdentCount, CScalarIdent::PopConvert(pexprScalarIdentCount->Pop())->PmdidType(), IMDType::EcmptEq);
		CExpression *pexprCountEqSum = CUtils::PexprScalarEqCmp(pmp, pcrCount, pcrSum);

		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDTypeInt8 *pmdtypeint8 = pmda->PtMDType<IMDTypeInt8>();
		IMDId *pmdidInt8 = pmdtypeint8->Pmdid();
		pmdidInt8->AddRef();
		pmdidInt8->AddRef();

		CExpression *pexprProjected =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CScalarIf(pmp, pmdidInt8),
				pexprCountEqZero,
				CUtils::PexprScalarConstInt8(pmp, 0 /*iVal*/),
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarIf(pmp, pmdidInt8),
					pexprCountEqSum,
					CUtils::PexprScalarConstInt8(pmp, -1 /*iVal*/),
					CUtils::PexprScalarIdent(pmp, pcrCount)
					)
			);
		CExpression *pexprPrj = CUtils::PexprAddProjection(pmp, pexprInnerNew, pexprProjected);

		const CColRef *pcrSubquery = CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr();
		*ppexprNewSubquery = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrSubquery, false /*fGeneratedByExist*/, true /*fGeneratedByQuantified*/), pexprPrj);
		*ppexprNewScalar = CUtils::PexprCmpWithZero(pmp, CUtils::PexprScalarIdent(pmp, pcrSubquery), pcrSubquery->Pmdtype()->Pmdid(), IMDType::EcmptG);
	}
	else
	{
		pexprInnerNew = CUtils::PexprCountStar(pmp, pexprSelect);
		const CColRef *pcrCount = CScalarProjectElement::PopConvert((*(*pexprInnerNew)[1])[0]->Pop())->Pcr();

		*ppexprNewSubquery = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrCount, false /*fGeneratedByExist*/, true /*fGeneratedByQuantified*/), pexprInnerNew);
		*ppexprNewScalar = CUtils::PexprCmpWithZero(pmp, CUtils::PexprScalarIdent(pmp, pcrCount), pcrCount->Pmdtype()->Pmdid(), IMDType::EcmptG);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::SubqueryAllToAgg
//
//	@doc:
//		Helper for transforming SubqueryAll into aggregate subquery,
//		we generate aggregate expressions that compute the following values:
//		- N: number of null values returned by evaluating inner expression
//		- S: number of inner values matching outer value
//		the generated subquery returns a Boolean result generated by the following
//		nested-if statement:
//
//			if (inner is empty)
//				return true
//			else if (N > 0)
//				return null
//			else if (outer value is null)
//				return null
//			else if (S == 0)
//				return true
//			else
//				return false
//
//
//---------------------------------------------------------------------------
void
CXformUtils::SubqueryAllToAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprSubquery,
	CExpression **ppexprNewSubquery, // output argument for new scalar subquery
	CExpression **ppexprNewScalar   // output argument for new scalar expression
	)
{
	GPOS_ASSERT(CUtils::FQuantifiedSubquery(pexprSubquery->Pop()));
	GPOS_ASSERT(COperator::EopScalarSubqueryAll == pexprSubquery->Pop()->Eopid());
	GPOS_ASSERT(NULL != ppexprNewSubquery);
	GPOS_ASSERT(NULL != ppexprNewScalar);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	CExpression *pexprInner = (*pexprSubquery)[0];
	CExpression *pexprScalarOuter = (*pexprSubquery)[1];
	CExpression *pexprSubqPred = PexprInversePred(pmp, pexprSubquery);

	// generate subquery test expression
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();
	IMDId *pmdidInt4 = pmdtypeint4->Pmdid();
	pmdidInt4->AddRef();
	CExpression *pexprSubqTest =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CScalarIf(pmp, pmdidInt4),
				pexprSubqPred,
				CUtils::PexprScalarConstInt4(pmp, 1 /*iVal*/),
				CUtils::PexprScalarConstInt4(pmp, 0 /*iVal*/)
				);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprSubqTest);

	// generate null indicator for inner expression
	const CColRef *pcrSubq = CScalarSubqueryQuantified::PopConvert(pexprSubquery->Pop())->Pcr();
	CExpression *pexprInnerNullIndicator = PexprNullIndicator(pmp, CUtils::PexprScalarIdent(pmp, pcrSubq));
	pdrgpexpr->Append(pexprInnerNullIndicator);

	// add generated expression as projected nodes
	pexprInner->AddRef();
	CExpression *pexprPrj = CUtils::PexprAddProjection(pmp, pexprInner, pdrgpexpr);
	pdrgpexpr->Release();

	// generate a group by expression with sum(subquery-test) and sum(inner null indicator) aggreagtes
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	CColRef *pcrSubqTest = const_cast<CColRef*>(CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[0]->Pop())->Pcr());
	CColRef *pcrInnerNullTest = const_cast<CColRef *>(CScalarProjectElement::PopConvert((*(*pexprPrj)[1])[1]->Pop())->Pcr());
	pdrgpcr->Append(pcrSubqTest);
	pdrgpcr->Append(pcrInnerNullTest);
	CExpression *pexprGbAggSum = CUtils::PexprGbAggSum(pmp, pexprPrj, pdrgpcr);
	pdrgpcr->Release();

	// generate helper test expressions
	const CColRef *pcrSum = CScalarProjectElement::PopConvert((*(*pexprGbAggSum)[1])[0]->Pop())->Pcr();
	const CColRef *pcrSumNulls = CScalarProjectElement::PopConvert((*(*pexprGbAggSum)[1])[1]->Pop())->Pcr();
	CExpression *pexprScalarIdentSum = CUtils::PexprScalarIdent(pmp, pcrSum);
	CExpression *pexprScalarIdentSumNulls = CUtils::PexprScalarIdent(pmp, pcrSumNulls);

	CExpression *pexprSumTest = CUtils::PexprCmpWithZero(pmp, pexprScalarIdentSum, CScalarIdent::PopConvert(pexprScalarIdentSum->Pop())->PmdidType(), IMDType::EcmptEq);
	pexprScalarIdentSum->AddRef();
	CExpression *pexprIsInnerEmpty = CUtils::PexprIsNull(pmp, pexprScalarIdentSum);
	CExpression *pexprInnerHasNulls = CUtils::PexprCmpWithZero(pmp, pexprScalarIdentSumNulls, CScalarIdent::PopConvert(pexprScalarIdentSumNulls->Pop())->PmdidType(), IMDType::EcmptG);
	pexprScalarOuter->AddRef();
	CExpression *pexprIsOuterNull = CUtils::PexprIsNull(pmp, pexprScalarOuter);

	// generate the main scalar if that will produce subquery result
	const IMDTypeBool *pmdtypebool = pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdidBool = pmdtypebool->Pmdid();
	pmdidBool->AddRef();
	pmdidBool->AddRef();
	pmdidBool->AddRef();
	pmdidBool->AddRef();
	pexprPrj =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarIf(pmp, pmdidBool),
			pexprIsInnerEmpty,
			CUtils::PexprScalarConstBool(pmp, true /*fVal*/), // if inner is empty, return true
			GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarIf(pmp, pmdidBool),
					pexprInnerHasNulls,
					CUtils::PexprScalarConstBool(pmp, false /*fVal*/, true /*fNull*/),	// if inner produced null values, return null
					GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CScalarIf(pmp, pmdidBool),
						pexprIsOuterNull,
						CUtils::PexprScalarConstBool(pmp, false /*fVal*/, true /*fNull*/), // if outer value is null, return null
						GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CScalarIf(pmp, pmdidBool),
							pexprSumTest,   // otherwise, test number of inner values that match outer value
							CUtils::PexprScalarConstBool(pmp, true /*fVal*/),  // no matches
							CUtils::PexprScalarConstBool(pmp, false /*fVal*/)  // at least one match
							)
						)
					)
			);

	CExpression *pexprProjected = CUtils::PexprAddProjection(pmp, pexprGbAggSum, pexprPrj);

	const CColRef *pcrSubquery = CScalarProjectElement::PopConvert((*(*pexprProjected)[1])[0]->Pop())->Pcr();
	*ppexprNewSubquery = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcrSubquery, false /*fGeneratedByExist*/, true /*fGeneratedByQuantified*/), pexprProjected);
	*ppexprNewScalar = CUtils::PexprScalarCmp(pmp, CUtils::PexprScalarIdent(pmp, pcrSubquery),CUtils::PexprScalarConstBool(pmp, true /*fVal*/), IMDType::EcmptEq);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprSeparateSubqueryPreds
//
//	@doc:
//		Helper function to separate subquery predicates in a top Select node
//
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprSeparateSubqueryPreds
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	COperator::EOperatorId eopid = pexpr->Pop()->Eopid();
	GPOS_ASSERT(COperator::EopLogicalInnerJoin == eopid ||
			COperator::EopLogicalNAryJoin == eopid);

	// split scalar expression into a conjunction of predicates with and without
	// subqueries
	const ULONG ulArity = pexpr->UlArity();
	CExpression *pexprScalar = (*pexpr)[ulArity - 1];
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	DrgPexpr *pdrgpexprSQ = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprNonSQ = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulConjuncts = pdrgpexprConjuncts->UlLength();
	for (ULONG ul = 0; ul < ulConjuncts; ul++)
	{
		CExpression *pexprConj = (*pdrgpexprConjuncts)[ul];
		pexprConj->AddRef();

		if (CDrvdPropScalar::Pdpscalar(pexprConj->PdpDerive())->FHasSubquery())
		{
			pdrgpexprSQ->Append(pexprConj);
		}
		else
		{
			pdrgpexprNonSQ->Append(pexprConj);
		}
	}
	GPOS_ASSERT(0 < pdrgpexprSQ->UlLength());

	pdrgpexprConjuncts->Release();

	// build children array from logical children and a conjunction of
	// non-subquery predicates
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}
	pdrgpexpr->Append(CPredicateUtils::PexprConjunction(pmp, pdrgpexprNonSQ));

	// build a new join
	COperator *popJoin = NULL;
	if (COperator::EopLogicalInnerJoin == eopid)
	{
		popJoin = GPOS_NEW(pmp) CLogicalInnerJoin(pmp);
	}
	else
	{
		popJoin = GPOS_NEW(pmp) CLogicalNAryJoin(pmp);
	}
	CExpression *pexprJoin = GPOS_NEW(pmp) CExpression(pmp, popJoin, pdrgpexpr);

	// return a Select node with a conjunction of subquery predicates
	return CUtils::PexprLogicalSelect(pmp, pexprJoin, CPredicateUtils::PexprConjunction(pmp, pdrgpexprSQ));
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprInversePred
//
//	@doc:
//		Helper for creating the inverse of the predicate used by
//		subquery ALL
//
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprInversePred
	(
	IMemoryPool *pmp,
	CExpression *pexprSubquery
	)
{
	// get the scalar child of subquery
	CScalarSubqueryAll *popSqAll = CScalarSubqueryAll::PopConvert(pexprSubquery->Pop());
	CExpression *pexprScalar = (*pexprSubquery)[1];
	const CColRef *pcr = popSqAll->Pcr();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// get mdid and name of the inverse of the comparison operator used by subquery
	IMDId *pmdidOp = popSqAll->PmdidOp();
	IMDId *pmdidInverseOp = pmda->Pmdscop(pmdidOp)->PmdidOpInverse();
	const CWStringConst *pstrFirst = pmda->Pmdscop(pmdidInverseOp)->Mdname().Pstr();

	// generate a predicate for the inversion of the comparison involved in the subquery
	pexprScalar->AddRef();
	pmdidInverseOp->AddRef();

	return CUtils::PexprScalarCmp(pmp, pexprScalar, pcr, *pstrFirst, pmdidInverseOp);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprNullIndicator
//
//	@doc:
//		Helper for creating a null indicator
//
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprNullIndicator
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	CExpression *pexprIsNull = CUtils::PexprIsNull(pmp, pexpr);
	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>();
	IMDId *pmdid = pmdtypeint4->Pmdid();
	pmdid->AddRef();
	return GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarIf(pmp, pmdid),
			pexprIsNull,
			CUtils::PexprScalarConstInt4(pmp, 1 /*iVal*/),
			CUtils::PexprScalarConstInt4(pmp, 0 /*iVal*/)
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprLogicalPartitionSelector
//
//	@doc:
// 		Create a logical partition selector for the given table descriptor on top
//		of the given child expression. The partition selection filters use columns
//		from the given column array
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprLogicalPartitionSelector
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcr,
	CExpression *pexprChild
	)
{
	IMDId *pmdidRel = ptabdesc->Pmdid();
	pmdidRel->AddRef();

	// create an oid column
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDTypeOid *pmdtype = pmda->PtMDType<IMDTypeOid>();
	CColRef *pcrOid = pcf->PcrCreate(pmdtype, IDefaultTypeModifier);
	DrgPexpr *pdrgpexprFilters = PdrgpexprPartEqFilters(pmp, ptabdesc, pdrgpcr);

	CLogicalPartitionSelector *popSelector = GPOS_NEW(pmp) CLogicalPartitionSelector(pmp, pmdidRel, pdrgpexprFilters, pcrOid);

	return GPOS_NEW(pmp) CExpression(pmp, popSelector, pexprChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprLogicalDMLOverProject
//
//	@doc:
// 		Create a logical DML on top of a project
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprLogicalDMLOverProject
	(
	IMemoryPool *pmp,
	CExpression *pexprChild,
	CLogicalDML::EDMLOperator edmlop,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcr,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId
	)
{
	GPOS_ASSERT(CLogicalDML::EdmlInsert == edmlop || CLogicalDML::EdmlDelete == edmlop);
	INT iVal = CScalarDMLAction::EdmlactionInsert;
	if (CLogicalDML::EdmlDelete == edmlop)
	{
		iVal = CScalarDMLAction::EdmlactionDelete;
	}

	// new expressions to project
	IMDId *pmdidRel = ptabdesc->Pmdid();
	CExpression *pexprProject = NULL;
	CColRef *pcrAction = NULL;
	CColRef *pcrOid = NULL;

	if (ptabdesc->FPartitioned())
	{
		// generate a PartitionSelector node which generates OIDs, then add a project
		// on top of that to add the action column
		CExpression *pexprSelector = PexprLogicalPartitionSelector(pmp, ptabdesc, pdrgpcr, pexprChild);
		if (CUtils::FGeneratePartOid(ptabdesc->Pmdid()))
		{
			pcrOid = CLogicalPartitionSelector::PopConvert(pexprSelector->Pop())->PcrOid();
		}
		pexprProject = CUtils::PexprAddProjection(pmp, pexprSelector, CUtils::PexprScalarConstInt4(pmp, iVal));
		CExpression *pexprPrL = (*pexprProject)[1];
		pcrAction = CUtils::PcrFromProjElem((*pexprPrL)[0]);
	}
	else
	{
		DrgPexpr *pdrgpexprProjected = GPOS_NEW(pmp) DrgPexpr(pmp);
		// generate one project node with two new columns: action, oid (based on the traceflag)
		pdrgpexprProjected->Append(CUtils::PexprScalarConstInt4(pmp, iVal));

		BOOL fGeneratePartOid = CUtils::FGeneratePartOid(ptabdesc->Pmdid());
		if (fGeneratePartOid)
		{
			OID oidTable = CMDIdGPDB::PmdidConvert(pmdidRel)->OidObjectId();
			pdrgpexprProjected->Append(CUtils::PexprScalarConstOid(pmp, oidTable));
		}

		pexprProject = CUtils::PexprAddProjection(pmp, pexprChild, pdrgpexprProjected);
		pdrgpexprProjected->Release();

		CExpression *pexprPrL = (*pexprProject)[1];
		pcrAction = CUtils::PcrFromProjElem((*pexprPrL)[0]);
		if (fGeneratePartOid)
		{
			pcrOid = CUtils::PcrFromProjElem((*pexprPrL)[1]);
		}
	}

	GPOS_ASSERT(NULL != pcrAction);

	if (FTriggersExist(edmlop, ptabdesc, true /*fBefore*/))
	{
		pmdidRel->AddRef();
		pexprProject = PexprRowTrigger(pmp, pexprProject, edmlop, pmdidRel, true /*fBefore*/, pdrgpcr);
	}

	if (CLogicalDML::EdmlInsert == edmlop)
	{
		// add assert for check constraints and nullness checks if needed
		COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
		if (poconf->Phint()->FEnforceConstraintsOnDML())
		{
			pexprProject = PexprAssertConstraints(pmp, pexprProject, ptabdesc, pdrgpcr);
		}
	}

	CExpression *pexprDML = GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalDML(pmp, edmlop, ptabdesc, pdrgpcr, GPOS_NEW(pmp) CBitSet(pmp) /*pbsModified*/, pcrAction, pcrOid, pcrCtid, pcrSegmentId, NULL /*pcrTupleOid*/),
			pexprProject
			);

	CExpression *pexprOutput = pexprDML;

	if (FTriggersExist(edmlop, ptabdesc, false /*fBefore*/))
	{
		pmdidRel->AddRef();
		pexprOutput = PexprRowTrigger(pmp, pexprOutput, edmlop, pmdidRel, false /*fBefore*/, pdrgpcr);
	}

	return pexprOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FTriggersExist
//
//	@doc:
//		Check whether there are any BEFORE or AFTER row-level triggers on
//		the given table that match the given DML operation
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FTriggersExist
	(
	CLogicalDML::EDMLOperator edmlop,
	CTableDescriptor *ptabdesc,
	BOOL fBefore
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());
	const ULONG ulTriggers = pmdrel->UlTriggers();

	for (ULONG ul = 0; ul < ulTriggers; ul++)
	{
		const IMDTrigger *pmdtrigger = pmda->Pmdtrigger(pmdrel->PmdidTrigger(ul));
		if (!pmdtrigger->FEnabled() ||
			!pmdtrigger->FRow() ||
			!FTriggerApplies(edmlop, pmdtrigger))
		{
			continue;
		}

		if (pmdtrigger->FBefore() == fBefore)
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FTriggerApplies
//
//	@doc:
//		Does the given trigger type match the given logical DML type
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FTriggerApplies
	(
	CLogicalDML::EDMLOperator edmlop,
	const IMDTrigger *pmdtrigger
	)
{
	return ((CLogicalDML::EdmlInsert == edmlop && pmdtrigger->FInsert()) ||
			(CLogicalDML::EdmlDelete == edmlop && pmdtrigger->FDelete()) ||
			(CLogicalDML::EdmlUpdate == edmlop && pmdtrigger->FUpdate()));
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprRowTrigger
//
//	@doc:
//		Construct a trigger expression on top of the given expression
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprRowTrigger
	(
	IMemoryPool *pmp,
	CExpression *pexprChild,
	CLogicalDML::EDMLOperator edmlop,
	IMDId *pmdidRel,
	BOOL fBefore,
	DrgPcr *pdrgpcr
	)
{
	GPOS_ASSERT(CLogicalDML::EdmlInsert == edmlop || CLogicalDML::EdmlDelete == edmlop);

	pdrgpcr->AddRef();
	if (CLogicalDML::EdmlInsert == edmlop)
	{
		return PexprRowTrigger(pmp, pexprChild, edmlop, pmdidRel, fBefore, NULL /*pdrgpcrOld*/, pdrgpcr);
	}

	return PexprRowTrigger(pmp, pexprChild, edmlop, pmdidRel, fBefore, pdrgpcr, NULL /*pdrgpcrNew*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprAssertNotNull
//
//	@doc:
//		Construct an assert on top of the given expression for nullness checks
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprAssertNotNull
	(
	IMemoryPool *pmp,
	CExpression *pexprChild,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcr
	)
{	
	DrgPcoldesc *pdrgpcoldesc = ptabdesc->Pdrgpcoldesc();
	
	const ULONG ulCols = pdrgpcoldesc->UlLength();
	CColRefSet *pcrsNotNull = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive())->PcrsNotNull();
	
	DrgPexpr *pdrgpexprAssertConstraints = GPOS_NEW(pmp) DrgPexpr(pmp);
	
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColumnDescriptor *pcoldesc = (*pdrgpcoldesc)[ul];
		if (pcoldesc->FNullable() || pcoldesc->FSystemColumn())
		{
			// target column is nullable or it's a system column: no need to check for NULL
			continue;
		}

		CColRef *pcr = (*pdrgpcr)[ul];

		if (pcrsNotNull->FMember(pcr))
		{
			// source column not nullable: no need to check for NULL
			continue;
		}
		
		// add not null check for current column
		CExpression *pexprNotNull = CUtils::PexprIsNotNull(pmp, CUtils::PexprScalarIdent(pmp, pcr));
		
		CWStringConst *pstrErrorMsg = PstrErrorMessage
										(
										pmp, 
										gpos::CException::ExmaSQL, 
										gpos::CException::ExmiSQLNotNullViolation,  
										pcoldesc->Name().Pstr()->Wsz(), 
										ptabdesc->Name().Pstr()->Wsz()
										);
		
		CExpression *pexprAssertConstraint = GPOS_NEW(pmp) CExpression
										(
										pmp, 
										GPOS_NEW(pmp) CScalarAssertConstraint(pmp, pstrErrorMsg), 
										pexprNotNull
										);
		
		pdrgpexprAssertConstraints->Append(pexprAssertConstraint);
	}
	
	if (0 == pdrgpexprAssertConstraints->UlLength())
	{
		pdrgpexprAssertConstraints->Release();
		return pexprChild;
	}
	
	CExpression *pexprAssertPredicate = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarAssertConstraintList(pmp), pdrgpexprAssertConstraints);

	CLogicalAssert *popAssert = 
			GPOS_NEW(pmp) CLogicalAssert
						(
						pmp, 
						GPOS_NEW(pmp) CException(gpos::CException::ExmaSQL, gpos::CException::ExmiSQLNotNullViolation)
						);
			
	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					popAssert,
					pexprChild,
					pexprAssertPredicate
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprRowTrigger
//
//	@doc:
//		Construct a trigger expression on top of the given expression
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprRowTrigger
	(
	IMemoryPool *pmp,
	CExpression *pexprChild,
	CLogicalDML::EDMLOperator edmlop,
	IMDId *pmdidRel,
	BOOL fBefore,
	DrgPcr *pdrgpcrOld,
	DrgPcr *pdrgpcrNew
	)
{
	INT iType = GPMD_TRIGGER_ROW;
	if (fBefore)
	{
		iType |= GPMD_TRIGGER_BEFORE;
	}

	switch (edmlop)
	{
		case CLogicalDML::EdmlInsert:
			iType |= GPMD_TRIGGER_INSERT;
			break;
		case CLogicalDML::EdmlDelete:
			iType |= GPMD_TRIGGER_DELETE;
			break;
		case CLogicalDML::EdmlUpdate:
			iType |= GPMD_TRIGGER_UPDATE;
			break;
		default:
			GPOS_ASSERT(!"Invalid DML operation");
	}

	return GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalRowTrigger(pmp, pmdidRel, iType, pdrgpcrOld, pdrgpcrNew),
			pexprChild
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpexprPartFilters
//
//	@doc:
// 		Return partition filter expressions for a DML operation given a table
//		descriptor and the column references seen by this DML
//
//---------------------------------------------------------------------------
DrgPexpr *
CXformUtils::PdrgpexprPartEqFilters
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrSource
	)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrSource);

	const DrgPul *pdrgpulPart = ptabdesc->PdrgpulPart();

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulPartKeys = pdrgpulPart->UlLength();
	GPOS_ASSERT(0 < ulPartKeys);
	GPOS_ASSERT(pdrgpcrSource->UlLength() >= ulPartKeys);

	for (ULONG ul = 0; ul < ulPartKeys; ul++)
	{
		ULONG *pulPartKey = (*pdrgpulPart)[ul];
		CColRef *pcr = (*pdrgpcrSource)[*pulPartKey];
		pdrgpexpr->Append(CUtils::PexprScalarIdent(pmp, pcr));
	}

	return pdrgpexpr;
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::PexprAssertConstraints
//
//      @doc:
//          Construct an assert on top of the given expression for check constraints
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprAssertConstraints
	(
	IMemoryPool *pmp,
	CExpression *pexprChild,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcr
	)
{
	CExpression *pexprAssertNotNull = PexprAssertNotNull(pmp, pexprChild, ptabdesc, pdrgpcr);

	return PexprAssertCheckConstraints(pmp, pexprAssertNotNull, ptabdesc, pdrgpcr);
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::PexprAssertCheckConstraints
//
//      @doc:
//          Construct an assert on top of the given expression for check constraints
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprAssertCheckConstraints
	(
	IMemoryPool *pmp,
	CExpression *pexprChild,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcr
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());

	const ULONG ulCheckConstraint = pmdrel->UlCheckConstraints();
	if (0 < ulCheckConstraint)
	{
	 	DrgPexpr *pdrgpexprAssertConstraints = GPOS_NEW(pmp) DrgPexpr(pmp);
	 	
		for (ULONG ul = 0; ul < ulCheckConstraint; ul++)
		{
			IMDId *pmdidCheckConstraint = pmdrel->PmdidCheckConstraint(ul);
			const IMDCheckConstraint *pmdCheckConstraint = pmda->Pmdcheckconstraint(pmdidCheckConstraint);

			// extract the check constraint expression
			CExpression *pexprCheckConstraint = pmdCheckConstraint->Pexpr(pmp, pmda, pdrgpcr);

			// A table check constraint is satisfied if and only if the specified <search condition>
			// evaluates to True or Unknown for every row of the table to which it applies.
			// Add an "is not false" expression on top to handle such scenarios
			CExpression *pexprIsNotFalse = CUtils::PexprIsNotFalse(pmp, pexprCheckConstraint);
			CWStringConst *pstrErrMsg = PstrErrorMessage
										(
										pmp, 
										gpos::CException::ExmaSQL, 
										gpos::CException::ExmiSQLCheckConstraintViolation, 
										pmdCheckConstraint->Mdname().Pstr()->Wsz(), 
										ptabdesc->Name().Pstr()->Wsz()
										);
			CExpression *pexprAssertConstraint = GPOS_NEW(pmp) CExpression
													(
													pmp,
													GPOS_NEW(pmp) CScalarAssertConstraint(pmp, pstrErrMsg),
													pexprIsNotFalse
													);
			
			pdrgpexprAssertConstraints->Append(pexprAssertConstraint);	
		}

	 	CExpression *pexprAssertPredicate = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarAssertConstraintList(pmp), pdrgpexprAssertConstraints);
	 	
		CLogicalAssert *popAssert = 
				GPOS_NEW(pmp) CLogicalAssert
							(
							pmp, 
							GPOS_NEW(pmp) CException(gpos::CException::ExmaSQL, gpos::CException::ExmiSQLCheckConstraintViolation)
							);
		

	 	return GPOS_NEW(pmp) CExpression
	 						(
	 						pmp,
	 						popAssert,
	 						pexprChild,
	 						pexprAssertPredicate
	 						);
	}

	return pexprChild;
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::PexprAssertUpdateCardinality
//
//      @doc:
//          Construct an assert on top of the given expression for checking cardinality
//			of updated values during DML UPDATE
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprAssertUpdateCardinality
	(
	IMemoryPool *pmp,
	CExpression *pexprDMLChild,
	CExpression *pexprDML,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId
	)
{
	COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
	CMDAccessor *pmda = poctxt->Pmda();
	
	CColRefSet *pcrsKey = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsKey->Include(pcrSegmentId);
	pcrsKey->Include(pcrCtid);
	
	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexprDMLChild->PdpDerive()); 
	CKeyCollection *pkc = pdprel->Pkc();
	if (NULL != pkc && pkc->FKey(pcrsKey))
	{
		// {segid, ctid} is a key: cardinality constraint is satisfied
		pcrsKey->Release();
		return pexprDML;
	}					
	
	pcrsKey->Release();
	
	// TODO:  - May 20, 2013; re-enable cardinality assert when the executor 
	// supports DML in a non-root slice
	
	GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedNonDeterministicUpdate);

	// construct a select(Action='DEL')
	CLogicalDML *popDML = CLogicalDML::PopConvert(pexprDML->Pop());
	CExpression *pexprConstDel = CUtils::PexprScalarConstInt4(pmp, CLogicalDML::EdmlDelete /*iVal*/);
	CExpression *pexprDelPredicate = CUtils::PexprScalarCmp(pmp, popDML->PcrAction(), pexprConstDel, IMDType::EcmptEq);
	CExpression *pexprSelectDeleted = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalSelect(pmp), pexprDML, pexprDelPredicate);
	// construct a group by	
	CColumnFactory *pcf = poctxt->Pcf();
		
	CExpression *pexprCountStar = CUtils::PexprCountStar(pmp);

	CScalar *pop = CScalar::PopConvert(pexprCountStar->Pop());
	const IMDType *pmdtype = pmda->Pmdtype(pop->PmdidType());
	CColRef *pcrProjElem = pcf->PcrCreate(pmdtype, pop->ITypeModifier());
	 
	CExpression *pexprProjElem = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarProjectElement(pmp, pcrProjElem),
									pexprCountStar
									);
	DrgPexpr *pdrgpexprProjElemsCountDistinct = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexprProjElemsCountDistinct->Append(pexprProjElem);
	CExpression *pexprProjList = GPOS_NEW(pmp) CExpression
								(
								pmp,
								GPOS_NEW(pmp) CScalarProjectList(pmp),
								pdrgpexprProjElemsCountDistinct
								);
								
	DrgPcr *pdrgpcrGbCols = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcrGbCols->Append(pcrCtid);
	pdrgpcrGbCols->Append(pcrSegmentId);
	
	CExpression *pexprGbAgg = GPOS_NEW(pmp) CExpression
								(
								pmp,
								GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrGbCols, COperator::EgbaggtypeGlobal /*egbaggtype*/),
								pexprSelectDeleted,
								pexprProjList
								);
	
	// construct a predicate of the kind "count(*) == 1"
	CExpression *pexprConst1 = CUtils::PexprScalarConstInt8(pmp, 1 /*iVal*/);
	// obtain error code and error message	
	CWStringConst *pstrErrorMsg = GPOS_NEW(pmp) CWStringConst(pmp, GPOS_WSZ_LIT("Duplicate values in UPDATE statement"));

	CExpression *pexprAssertConstraint = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarAssertConstraint(pmp, pstrErrorMsg),
											CUtils::PexprScalarCmp(pmp, pcrProjElem, pexprConst1, IMDType::EcmptEq)
											);
	
	CExpression *pexprAssertPredicate = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarAssertConstraintList(pmp), pexprAssertConstraint);
		
	return GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalAssert
								(
								pmp, 
								GPOS_NEW(pmp) CException(gpos::CException::ExmaSQL, gpos::CException::ExmiSQLDefault)
								),
						pexprGbAgg,
						pexprAssertPredicate
						);	
}

//---------------------------------------------------------------------------
//   @function:
//		CXformUtils::FSupportsMinAgg
//
//   @doc:
//      Check if all column types support MIN aggregate
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FSupportsMinAgg
	(
	DrgPcr *pdrgpcr
	)
{
	const ULONG ulCols = pdrgpcr->UlLength();
	
	// add the columns to project list
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		const IMDType *pmdtype = pcr->Pmdtype();
		if (!IMDId::FValid(pmdtype->PmdidAgg(IMDType::EaggMin)))
		{
			return false;
		}
	}
	return true;
}


//---------------------------------------------------------------------------
//   @function:
//		CXformUtils::FSplitAggXform
//
//   @doc:
//      Check if given xform is an Agg splitting xform
//
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FSplitAggXform
	(
	CXform::EXformId exfid
	)
{
	return
		CXform::ExfSplitGbAgg == exfid ||
		CXform::ExfSplitDQA == exfid ||
		CXform::ExfSplitGbAggDedup == exfid;
}


//---------------------------------------------------------------------------
//   @function:
//		CXformUtils::FMultiStageAgg
//
//   @doc:
//      Check if given expression is a multi-stage Agg based on agg type
//		or origin xform
//
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FMultiStageAgg
	(
	CExpression *pexprAgg
	)
{
	GPOS_ASSERT(NULL != pexprAgg);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexprAgg->Pop()->Eopid() ||
			COperator::EopLogicalGbAggDeduplicate == pexprAgg->Pop()->Eopid());

	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexprAgg->Pop());
	if (COperator::EgbaggtypeGlobal != popAgg->Egbaggtype())
	{
		// a non-global agg is a multi-stage agg
		return true;
	}

	// check xform lineage
	BOOL fMultiStage = false;
	CGroupExpression *pgexprOrigin = pexprAgg->Pgexpr();
	while (NULL != pgexprOrigin && !fMultiStage)
	{
		fMultiStage = FSplitAggXform(pgexprOrigin->ExfidOrigin());
		pgexprOrigin = pgexprOrigin->PgexprOrigin();
	}

	return fMultiStage;
}


//---------------------------------------------------------------------------
//   @function:
//		CXformUtils::AddMinAggs
//
//   @doc:
//      Add a min(col) project element for each column in the given array to the
//		given expression array
//		
//
//---------------------------------------------------------------------------
void
CXformUtils::AddMinAggs
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CColumnFactory *pcf,
	DrgPcr *pdrgpcr,
	HMCrCr *phmcrcr,
	DrgPexpr *pdrgpexpr,
	DrgPcr **ppdrgpcrNew
	)
{
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(NULL != phmcrcr);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != ppdrgpcrNew);
	
	const ULONG ulCols = pdrgpcr->UlLength();
	
	// add the columns to project list
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		
		CColRef *pcrNew = phmcrcr->PtLookup(pcr);
		
		if (NULL == pcrNew)
		{
			// construct min(col) aggregate
			CExpression *pexprMinAgg = CUtils::PexprMin(pmp, pmda, pcr);
			CScalar *popMin = CScalar::PopConvert(pexprMinAgg->Pop());
			
			const IMDType *pmdtypeMin = pmda->Pmdtype(popMin->PmdidType());
			pcrNew = pcf->PcrCreate(pmdtypeMin, popMin->ITypeModifier());
			CExpression *pexprProjElemMin = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarProjectElement(pmp, pcrNew),
											pexprMinAgg
											);
			
			pdrgpexpr->Append(pexprProjElemMin);
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmcrcr->FInsert(pcr, pcrNew);
			GPOS_ASSERT(fResult);
		}
		(*ppdrgpcrNew)->Append(pcrNew);
	}
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FXformInArray
//
//      @doc:
//          Check if given xform id is in the given array of xforms
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FXformInArray
	(
	CXform::EXformId exfid,
	CXform::EXformId rgXforms[],
	ULONG ulXforms
	)
{
	for (ULONG ul = 0; ul < ulXforms; ul++)
	{
		if (rgXforms[ul] == exfid)
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FDeriveStatsBeforeXform
//
//      @doc:
//          Return true if stats derivation is needed for this xform
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FDeriveStatsBeforeXform
	(
	CXform *pxform
	)
{
	GPOS_ASSERT(NULL != pxform);

	return pxform->FExploration() &&
			CXformExploration::Pxformexp(pxform)->FNeedsStats();
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FSubqueryDecorrelation
//
//      @doc:
//          Check if xform is a subquery decorrelation xform
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FSubqueryDecorrelation
	(
	CXform *pxform
	)
{
	GPOS_ASSERT(NULL != pxform);

	return pxform->FExploration() &&
			CXformExploration::Pxformexp(pxform)->FApplyDecorrelating();
}


//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FSubqueryUnnesting
//
//      @doc:
//          Check if xform is a subquery unnesting xform
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FSubqueryUnnesting
	(
	CXform *pxform
	)
{
	GPOS_ASSERT(NULL != pxform);

	return pxform->FExploration() &&
			CXformExploration::Pxformexp(pxform)->FSubqueryUnnesting();
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::FApplyToNextBinding
//
//      @doc:
//         Check if xform should be applied to the next binding
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FApplyToNextBinding
	(
	CXform *pxform,
	CExpression *pexprLastBinding // last extracted xform pattern
	)
{
	GPOS_ASSERT(NULL != pxform);

	if (FSubqueryDecorrelation(pxform))
	{
		// if last binding is free from Subquery or Apply operators, we do not
		// need to apply the xform further
		return CUtils::FHasSubqueryOrApply(pexprLastBinding, false /*fCheckRoot*/) ||
				CUtils::FHasCorrelatedApply(pexprLastBinding, false /*fCheckRoot*/);
	}

	// set of transformations that should be applied once
	CXform::EXformId rgXforms[] =
	{
		CXform::ExfJoinAssociativity,
		CXform::ExfExpandFullOuterJoin,
		CXform::ExfUnnestTVF,
		CXform::ExfLeftSemiJoin2CrossProduct,
		CXform::ExfLeftAntiSemiJoin2CrossProduct,
		CXform::ExfLeftAntiSemiJoinNotIn2CrossProduct,
	};

	CXform::EXformId exfid = pxform->Exfid();

	BOOL fApplyOnce =
		FSubqueryUnnesting(pxform) ||
		FXformInArray(exfid, rgXforms, GPOS_ARRAY_SIZE(rgXforms));

	return !fApplyOnce;
}

//---------------------------------------------------------------------------
//      @function:
//              CXformUtils::PstrErrorMessage
//
//      @doc:
//          Compute an error message for given exception type
//
//---------------------------------------------------------------------------
CWStringConst *
CXformUtils::PstrErrorMessage
	(
	IMemoryPool *pmp,
	ULONG ulMajor,
	ULONG ulMinor,
	...
	)
{
	WCHAR wsz[1024];
	CWStringStatic str(wsz, 1024);
	
	// manufacture actual exception object
	CException exc(ulMajor, ulMinor);
	
	// during bootstrap there's no context object otherwise, record
	// all details in the context object
	if (NULL != ITask::PtskSelf())
	{
		VA_LIST valist;
		VA_START(valist, ulMinor);

		ELocale eloc = ITask::PtskSelf()->Eloc();
		CMessage *pmsg = CMessageRepository::Pmr()->PmsgLookup(exc, eloc);
		pmsg->Format(&str, valist);

		VA_END(valist);
	}	

	return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpcrIndexKeys
//
//	@doc:
//		Return the array of columns from the given array of columns which appear 
//		in the index key columns
//
//---------------------------------------------------------------------------
DrgPcr *
CXformUtils::PdrgpcrIndexKeys
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel
	)
{
	return PdrgpcrIndexColumns(pmp, pdrgpcr, pmdindex, pmdrel, EicKey);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PcrsIndexKeys
//
//	@doc:
//		Return the set of columns from the given array of columns which appear 
//		in the index key columns
//
//---------------------------------------------------------------------------
CColRefSet *
CXformUtils::PcrsIndexKeys
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel
	)
{
	return PcrsIndexColumns(pmp, pdrgpcr, pmdindex, pmdrel, EicKey);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PcrsIndexIncludedCols
//
//	@doc:
//		Return the set of columns from the given array of columns which appear 
//		in the index included columns
//
//---------------------------------------------------------------------------
CColRefSet *
CXformUtils::PcrsIndexIncludedCols
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel
	)
{
	return PcrsIndexColumns(pmp, pdrgpcr, pmdindex, pmdrel, EicIncluded);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PcrsIndexColumns
//
//	@doc:
//		Return the set of columns from the given array of columns which appear 
//		in the index columns of the specified type (included / key)
//
//---------------------------------------------------------------------------
CColRefSet *
CXformUtils::PcrsIndexColumns
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	EIndexCols eic
	)
{
	GPOS_ASSERT(EicKey == eic || EicIncluded == eic);
	DrgPcr *pdrgpcrIndexColumns = PdrgpcrIndexColumns(pmp, pdrgpcr, pmdindex, pmdrel, eic);
	CColRefSet *pcrsCols = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrIndexColumns);

	pdrgpcrIndexColumns->Release();
	
	return pcrsCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpcrIndexColumns
//
//	@doc:
//		Return the ordered list of columns from the given array of columns which  
//		appear in the index columns of the specified type (included / key)
//
//---------------------------------------------------------------------------
DrgPcr *
CXformUtils::PdrgpcrIndexColumns
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	EIndexCols eic
	)
{
	GPOS_ASSERT(EicKey == eic || EicIncluded == eic);
	
	DrgPcr *pdrgpcrIndex = GPOS_NEW(pmp) DrgPcr(pmp);

	ULONG ulLength = pmdindex->UlKeys();
	if (EicIncluded == eic)
	{
		ulLength = pmdindex->UlIncludedCols();
	}

	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		ULONG ulPos = gpos::ulong_max;
		if (EicIncluded == eic)
		{
			ulPos = pmdindex->UlIncludedCol(ul);
		}
		else
		{
			ulPos = pmdindex->UlKey(ul);
		}
		ULONG ulPosNonDropped = pmdrel->UlPosNonDropped(ulPos);

		GPOS_ASSERT(gpos::ulong_max != ulPosNonDropped);
		GPOS_ASSERT(ulPosNonDropped < pdrgpcr->UlLength());

		CColRef *pcr = (*pdrgpcr)[ulPosNonDropped];
		pdrgpcrIndex->Append(pcr);
	}

	return pdrgpcrIndex;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FIndexApplicable
//
//	@doc:
//		Check if an index is applicable given the required, output and scalar
//		expression columns
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FIndexApplicable
	(
	IMemoryPool *pmp,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsReqd,
	CColRefSet *pcrsScalar,
	IMDIndex::EmdindexType emdindtype
	)
{
	if (emdindtype != pmdindex->Emdindt() ||
		0 == pcrsScalar->CElements()) // no columns to match index against
	{
		return false;
	}
	
	BOOL fApplicable = true;
	
	CColRefSet *pcrsIncludedCols = CXformUtils::PcrsIndexIncludedCols(pmp, pdrgpcrOutput, pmdindex, pmdrel);
	CColRefSet *pcrsIndexCols = CXformUtils::PcrsIndexKeys(pmp, pdrgpcrOutput, pmdindex, pmdrel);
	if (!pcrsIncludedCols->FSubset(pcrsReqd) || // index is not covering
		pcrsScalar->FDisjoint(pcrsIndexCols)) // indexing columns disjoint from the columns used in the scalar expression
	{
		fApplicable = false;
	}
	
	// clean up
	pcrsIncludedCols->Release();
	pcrsIndexCols->Release();

	return fApplicable;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprRowNumber
//
//	@doc:
//		Create an expression with "row_number" window function
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprRowNumber
	(
	IMemoryPool *pmp
	)
{

	OID oidRowNumber = COptCtxt::PoctxtFromTLS()->Poconf()->Pwindowoids()->OidRowNumber();

	CScalarWindowFunc *popRowNumber = GPOS_NEW(pmp) CScalarWindowFunc
													(
													pmp,
													GPOS_NEW(pmp) CMDIdGPDB(oidRowNumber),
													GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_OID),
													GPOS_NEW(pmp) CWStringConst(pmp, GPOS_WSZ_LIT("row_number")),
													CScalarWindowFunc::EwsImmediate,
													false /* fDistinct */,
													false /* fStarArg */,
													false /* fSimpleAgg */
													);

	CExpression *pexprScRowNumber = GPOS_NEW(pmp) CExpression(pmp, popRowNumber);

	return pexprScRowNumber;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprWindowWithRowNumber
//
//	@doc:
//		Create a sequence project (window) expression with a row_number
//		window function and partitioned by the given array of columns references
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprWindowWithRowNumber
	(
	IMemoryPool *pmp,
	CExpression *pexprWindowChild,
	DrgPcr *pdrgpcrInput
	)
{
	// partitioning information
	CDistributionSpec *pds = NULL;
	if (NULL != pdrgpcrInput)
	{
		DrgPexpr *pdrgpexprInput = CUtils::PdrgpexprScalarIdents(pmp, pdrgpcrInput);
		pds = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprInput, true /* fNullsCollocated */);
	}
	else
	{
		 pds = GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);
	}

	// window frames
	DrgPwf *pdrgpwf = GPOS_NEW(pmp) DrgPwf(pmp);

	// ordering information
	DrgPos *pdrgpos = GPOS_NEW(pmp) DrgPos(pmp);

	// row_number window function project list
	CExpression *pexprScWindowFunc = PexprRowNumber(pmp);

	// generate a new column reference
	CScalarWindowFunc *popScWindowFunc = CScalarWindowFunc::PopConvert(pexprScWindowFunc->Pop());
	const IMDType *pmdtype = COptCtxt::PoctxtFromTLS()->Pmda()->Pmdtype(popScWindowFunc->PmdidType());
	CName name(popScWindowFunc->PstrFunc());
	CColRef *pcr = COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pmdtype, popScWindowFunc->ITypeModifier(), name);

	// new project element
	CScalarProjectElement *popScPrEl = GPOS_NEW(pmp) CScalarProjectElement(pmp, pcr);

	// generate a project element
	CExpression *pexprProjElem = GPOS_NEW(pmp) CExpression(pmp, popScPrEl, pexprScWindowFunc);

	// generate the project list
	CExpression *pexprProjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprProjElem);

	CLogicalSequenceProject *popLgSequence = GPOS_NEW(pmp) CLogicalSequenceProject(pmp, pds, pdrgpos, pdrgpwf);

	pexprWindowChild->AddRef();
	CExpression *pexprLgSequence =  GPOS_NEW(pmp) CExpression(pmp, popLgSequence, pexprWindowChild, pexprProjList);

	return pexprLgSequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprAssertOneRow
//
//	@doc:
//		Generate a logical Assert expression that errors out when more than
//		one row is generated
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprAssertOneRow
	(
	IMemoryPool *pmp,
	CExpression *pexprChild
	)
{
	GPOS_ASSERT(NULL != pexprChild);
	GPOS_ASSERT(pexprChild->Pop()->FLogical());

	CExpression *pexprSeqPrj = PexprWindowWithRowNumber(pmp, pexprChild, NULL /*pdrgpcrInput*/);
	CColRef *pcrRowNumber = CScalarProjectElement::PopConvert((*(*pexprSeqPrj)[1])[0]->Pop())->Pcr();
	CExpression *pexprCmp = CUtils::PexprScalarEqCmp(pmp, pcrRowNumber, CUtils::PexprScalarConstInt4(pmp, 1 /*fVal*/));

	CWStringConst *pstrErrorMsg = PstrErrorMessage(pmp, gpos::CException::ExmaSQL, gpos::CException::ExmiSQLMaxOneRow);
	CExpression *pexprAssertConstraint = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarAssertConstraint(pmp, pstrErrorMsg),
											pexprCmp
											);

	CExpression *pexprAssertPredicate = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarAssertConstraintList(pmp), pexprAssertConstraint);
		
	CLogicalAssert *popAssert =
		GPOS_NEW(pmp) CLogicalAssert
			(
			pmp,
			GPOS_NEW(pmp) CException(gpos::CException::ExmaSQL, gpos::CException::ExmiSQLMaxOneRow)
			);

	return GPOS_NEW(pmp) CExpression(pmp, popAssert, pexprSeqPrj, pexprAssertPredicate);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PcrProjectElement
//
//	@doc:
//		Return the pcr of the n-th project element
//---------------------------------------------------------------------------
CColRef *
CXformUtils::PcrProjectElement
	(
	CExpression *pexpr,
	ULONG ulIdxProjElement
	)
{
	CExpression *pexprProjList = (*pexpr)[1];
	GPOS_ASSERT(COperator::EopScalarProjectList == pexprProjList->Pop()->Eopid());

	CExpression *pexprProjElement = (*pexprProjList)[ulIdxProjElement];
	GPOS_ASSERT(NULL != pexprProjElement);

	return CScalarProjectElement::PopConvert(pexprProjElement->Pop())->Pcr();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::LookupHashJoinKeys
//
//	@doc:
//		Lookup hash join keys in scalar child group
//
//---------------------------------------------------------------------------
void
CXformUtils::LookupHashJoinKeys
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPexpr **ppdrgpexprOuter,
	DrgPexpr **ppdrgpexprInner
	)
{
	GPOS_ASSERT(NULL != ppdrgpexprOuter);
	GPOS_ASSERT(NULL != ppdrgpexprInner);

	*ppdrgpexprOuter = NULL;
	*ppdrgpexprInner = NULL;
	CGroupExpression *pgexprScalarOrigin = (*pexpr)[2]->Pgexpr();
	if (NULL == pgexprScalarOrigin)
	{
		return;
	}

	CColRefSet *pcrsOuterOutput = CDrvdPropRelational::Pdprel((*pexpr)[0]->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::Pdprel((*pexpr)[1]->PdpDerive())->PcrsOutput();

	CGroup *pgroupScalar = pgexprScalarOrigin->Pgroup();
	if (NULL == pgroupScalar->PdrgpexprHashJoinKeysOuter())
	{
		// hash join keys not found
		return;
	}

	GPOS_ASSERT(NULL != pgroupScalar->PdrgpexprHashJoinKeysInner());

	// extract used columns by hash join keys
	CColRefSet *pcrsUsedOuter = CUtils::PcrsExtractColumns(pmp, pgroupScalar->PdrgpexprHashJoinKeysOuter());
	CColRefSet *pcrsUsedInner = CUtils::PcrsExtractColumns(pmp, pgroupScalar->PdrgpexprHashJoinKeysInner());

	BOOL fOuterKeysUsesOuterChild = pcrsOuterOutput->FSubset(pcrsUsedOuter);
	BOOL fInnerKeysUsesInnerChild = pcrsInnerOutput->FSubset(pcrsUsedInner);
	BOOL fInnerKeysUsesOuterChild = pcrsOuterOutput->FSubset(pcrsUsedInner);
	BOOL fOuterKeysUsesInnerChild = pcrsInnerOutput->FSubset(pcrsUsedOuter);

	if ((fOuterKeysUsesOuterChild && fInnerKeysUsesInnerChild) ||
		(fInnerKeysUsesOuterChild && fOuterKeysUsesInnerChild))
	{
		CGroupProxy gp(pgroupScalar);

		pgroupScalar->PdrgpexprHashJoinKeysOuter()->AddRef();
		pgroupScalar->PdrgpexprHashJoinKeysInner()->AddRef();

		// align hash join keys with join child
		if (fOuterKeysUsesOuterChild && fInnerKeysUsesInnerChild)
		{
			*ppdrgpexprOuter = pgroupScalar->PdrgpexprHashJoinKeysOuter();
			*ppdrgpexprInner = pgroupScalar->PdrgpexprHashJoinKeysInner();
		}
		else
		{
			GPOS_ASSERT(fInnerKeysUsesOuterChild && fOuterKeysUsesInnerChild);

			*ppdrgpexprOuter = pgroupScalar->PdrgpexprHashJoinKeysInner();
			*ppdrgpexprInner = pgroupScalar->PdrgpexprHashJoinKeysOuter();
		}
	}

	pcrsUsedOuter->Release();
	pcrsUsedInner->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::CacheHashJoinKeys
//
//	@doc:
//		Cache hash join keys on scalar child group
//
//---------------------------------------------------------------------------
void
CXformUtils::CacheHashJoinKeys
	(
	CExpression *pexpr,
	DrgPexpr *pdrgpexprOuter,
	DrgPexpr *pdrgpexprInner
	)
{
	GPOS_ASSERT(NULL != pdrgpexprOuter);
	GPOS_ASSERT(NULL != pdrgpexprInner);

	CGroupExpression *pgexprScalarOrigin = (*pexpr)[2]->Pgexpr();
	if (NULL != pgexprScalarOrigin)
	{
		CGroup *pgroupScalar = pgexprScalarOrigin->Pgroup();

		{	// scope of group proxy
			CGroupProxy gp(pgroupScalar);
			gp.SetHashJoinKeys(pdrgpexprOuter, pdrgpexprInner);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::AddCTEProducer
//
//	@doc:
//		Helper to create a CTE producer expression and add it to global
//		CTE info structure
//		Does not take ownership of pexpr
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprAddCTEProducer
	(
	IMemoryPool *pmp,
	ULONG ulCTEId,
	DrgPcr *pdrgpcr,
	CExpression *pexpr
	)
{
	DrgPcr *pdrgpcrProd = CUtils::PdrgpcrCopy(pmp, pdrgpcr);
	HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, pdrgpcr, pdrgpcrProd);
	CExpression *pexprRemapped = pexpr->PexprCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
	phmulcr->Release();

	CExpression *pexprProducer =
			GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CLogicalCTEProducer(pmp, ulCTEId, pdrgpcrProd),
							pexprRemapped
							);

	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	pcteinfo->AddCTEProducer(pexprProducer);
	pexprProducer->Release();

	return pcteinfo->PexprCTEProducer(ulCTEId);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FProcessGPDBAntiSemiHashJoin
//
//	@doc:
//		Helper to extract equality from an expression tree of the form
//		OP
//		 |--(=)
//		 |	 |-- expr1
//		 |	 +-- expr2
//		 +--exprOther
//
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FExtractEquality
	(
	CExpression *pexpr,
	CExpression **ppexprEquality, // output: extracted equality expression, set to NULL if extraction failed
	CExpression **ppexprOther // output: sibling of equality expression, set to NULL if extraction failed
	)
{
	GPOS_ASSERT(2 == pexpr->UlArity());

	*ppexprEquality = NULL;
	*ppexprOther = NULL;

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];
	BOOL fEqualityOnLeft = CPredicateUtils::FEquality(pexprLeft);
	BOOL fEqualityOnRight = CPredicateUtils::FEquality(pexprRight);
	if (fEqualityOnLeft || fEqualityOnRight)
	{
		*ppexprEquality = pexprLeft;
		*ppexprOther = pexprRight;
		if (fEqualityOnRight)
		{
			*ppexprEquality = pexprRight;
			*ppexprOther = pexprLeft;
		}

		return true;
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FProcessGPDBAntiSemiHashJoin
//
//	@doc:
//		GPDB hash join return no results if the inner side of anti-semi-join
//		produces null values, this allows simplifying join predicates of the
//		form (equality_expr IS DISTINCT FROM false) to (equality_expr) since
//		GPDB hash join operator guarantees no join results to be returned in
//		this case
//
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FProcessGPDBAntiSemiHashJoin
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression **ppexprResult // output: result expression, set to NULL if processing failed
	)
{
	GPOS_ASSERT(NULL != ppexprResult);
	GPOS_ASSERT(COperator::EopLogicalLeftAntiSemiJoin == pexpr->Pop()->Eopid() ||
				COperator::EopLogicalLeftAntiSemiJoinNotIn == pexpr->Pop()->Eopid());

	*ppexprResult = NULL;
	CExpression *pexprOuter = (*pexpr)[0];
	CExpression *pexprInner = (*pexpr)[1];
	CExpression *pexprScalar = (*pexpr)[2];

	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	DrgPexpr *pdrgpexprNew = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulPreds = pdrgpexpr->UlLength();
	BOOL fSimplifiedPredicate = false;
	for (ULONG ul = 0; ul < ulPreds; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		if (CPredicateUtils::FIDFFalse(pexprPred))
		{
			CExpression *pexprEquality = NULL;
			CExpression *pexprFalse = NULL;
			if (FExtractEquality(pexprPred, &pexprEquality, &pexprFalse) &&  // extracted equality expression
				IMDId::EmdidGPDB == CScalarConst::PopConvert(pexprFalse->Pop())->Pdatum()->Pmdid()->Emdidt() && // underlying system is GPDB
				CPhysicalJoin::FHashJoinCompatible(pexprEquality, pexprOuter, pexprInner) && // equality is hash-join compatible
				CUtils::FUsesNullableCol(pmp, pexprEquality, pexprInner)) // equality uses an inner nullable column
				{
					pexprEquality->AddRef();
					pdrgpexprNew->Append(pexprEquality);
					fSimplifiedPredicate = true;
					continue;
				}
		}
		pexprPred->AddRef();
		pdrgpexprNew->Append(pexprPred);
	}

	pdrgpexpr->Release();
	if (!fSimplifiedPredicate)
	{
		pdrgpexprNew->Release();
		return false;
	}

	pexprOuter->AddRef();
	pexprInner->AddRef();
	pexpr->Pop()->AddRef();
	*ppexprResult = GPOS_NEW(pmp) 		CExpression
			(
			pmp,
			pexpr->Pop(),
			pexprOuter,
			pexprInner,
			CPredicateUtils::PexprConjunction(pmp, pdrgpexprNew)
			);

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBuildIndexPlan
//
//	@doc:
//		Construct an expression representing a new access path using the given functors for
//		operator constructors and rewritten access path.
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBuildIndexPlan
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CExpression *pexprGet,
	ULONG ulOriginOpId,
	DrgPexpr *pdrgpexprConds,
	CColRefSet *pcrsReqd,
	CColRefSet *pcrsScalarExpr,
	CColRefSet *pcrsOuterRefs,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	BOOL fAllowPartialIndex,
	CPartConstraint *ppartcnstrIndex,
	IMDIndex::EmdindexType emdindtype,
	PDynamicIndexOpConstructor pdiopc,
	PStaticIndexOpConstructor psiopc,
	PRewrittenIndexPath prip
	)
{
	GPOS_ASSERT(NULL != pexprGet);
	GPOS_ASSERT(NULL != pdrgpexprConds);
	GPOS_ASSERT(NULL != pcrsReqd);
	GPOS_ASSERT(NULL != pcrsScalarExpr);
	GPOS_ASSERT(NULL != pmdindex);
	GPOS_ASSERT(NULL != pmdrel);

	COperator::EOperatorId eopid = pexprGet->Pop()->Eopid();
	GPOS_ASSERT(CLogical::EopLogicalGet == eopid || CLogical::EopLogicalDynamicGet == eopid);

	BOOL fDynamicGet = (COperator::EopLogicalDynamicGet == eopid);
	GPOS_ASSERT_IMP(!fDynamicGet, NULL == ppartcnstrIndex);

	CTableDescriptor *ptabdesc = NULL;
	DrgPcr *pdrgpcrOutput = NULL;
	CWStringConst *pstrAlias = NULL;
	ULONG ulPartIndex = gpos::ulong_max;
	DrgDrgPcr *pdrgpdrgpcrPart = NULL;
	BOOL fPartialIndex = pmdrel->FPartialIndex(pmdindex->Pmdid());
	ULONG ulSecondaryPartIndex = gpos::ulong_max;
	CPartConstraint *ppartcnstrRel = NULL;

	if (!fAllowPartialIndex && fPartialIndex)
	{
		CRefCount::SafeRelease(ppartcnstrIndex);

		// partial indexes are not allowed
		return NULL;
	}

	if (fDynamicGet)
	{
		CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(pexprGet->Pop());

		ptabdesc = popDynamicGet->Ptabdesc();
		ulPartIndex = popDynamicGet->UlScanId();
		pdrgpcrOutput = popDynamicGet->PdrgpcrOutput();
		GPOS_ASSERT(NULL != pdrgpcrOutput);
		pstrAlias = GPOS_NEW(pmp) CWStringConst(pmp, popDynamicGet->Name().Pstr()->Wsz());
		pdrgpdrgpcrPart = popDynamicGet->PdrgpdrgpcrPart();
		ulSecondaryPartIndex = popDynamicGet->UlSecondaryScanId();
		ppartcnstrRel = popDynamicGet->PpartcnstrRel();
	}
	else
	{
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprGet->Pop());
		ptabdesc = popGet->Ptabdesc();
		pdrgpcrOutput = popGet->PdrgpcrOutput();
		GPOS_ASSERT(NULL != pdrgpcrOutput);
		pstrAlias = GPOS_NEW(pmp) CWStringConst(pmp, popGet->Name().Pstr()->Wsz());
	}

	if (!FIndexApplicable(pmp, pmdindex, pmdrel, pdrgpcrOutput, pcrsReqd, pcrsScalarExpr, emdindtype))
	{
		GPOS_DELETE(pstrAlias);
		CRefCount::SafeRelease(ppartcnstrIndex);

		return NULL;
	}

	DrgPcr *pdrgppcrIndexCols = PdrgpcrIndexKeys(pmp, pdrgpcrOutput, pmdindex, pmdrel);
	DrgPexpr *pdrgpexprIndex = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprResidual = GPOS_NEW(pmp) DrgPexpr(pmp);
	CPredicateUtils::ExtractIndexPredicates(pmp, pmda, pdrgpexprConds, pmdindex, pdrgppcrIndexCols, pdrgpexprIndex, pdrgpexprResidual, pcrsOuterRefs);

	if (0 == pdrgpexprIndex->UlLength())
	{
		// clean up
		GPOS_DELETE(pstrAlias);
		pdrgppcrIndexCols->Release();
		pdrgpexprResidual->Release();
		pdrgpexprIndex->Release();
		CRefCount::SafeRelease(ppartcnstrIndex);

		return NULL;
	}
	GPOS_ASSERT(pdrgpexprConds->UlLength() == pdrgpexprResidual->UlLength() + pdrgpexprIndex->UlLength());

	ptabdesc->AddRef();
	pdrgpcrOutput->AddRef();
	// create the logical (dynamic) bitmap table get operator
	CLogical *popLogicalGet =	NULL;

	if (fDynamicGet)
	{
		pdrgpdrgpcrPart->AddRef();
		ppartcnstrRel->AddRef();
		popLogicalGet = (*pdiopc)
						(
						pmp,
						pmdindex,
						ptabdesc,
						ulOriginOpId,
						GPOS_NEW(pmp) CName(pmp, CName(pstrAlias)),
						ulPartIndex,
						pdrgpcrOutput,
						pdrgpdrgpcrPart,
						ulSecondaryPartIndex,
						ppartcnstrIndex,
						ppartcnstrRel
						);
	}
	else
	{
		popLogicalGet = (*psiopc)
						(
						pmp,
						pmdindex,
						ptabdesc,
						ulOriginOpId,
						GPOS_NEW(pmp) CName(pmp, CName(pstrAlias)),
						pdrgpcrOutput
						);
	}

	// clean up
	GPOS_DELETE(pstrAlias);
	pdrgppcrIndexCols->Release();

	CExpression *pexprIndexCond = CPredicateUtils::PexprConjunction(pmp, pdrgpexprIndex);
	CExpression *pexprResidualCond = CPredicateUtils::PexprConjunction(pmp, pdrgpexprResidual);

	return (*prip)(pmp, pexprIndexCond, pexprResidualCond, pmdindex, ptabdesc, popLogicalGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprScalarBitmapBoolOp
//
//	@doc:
//		 Helper for creating BitmapBoolOp expression
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprScalarBitmapBoolOp
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CExpression *pexprOriginalPred,
	DrgPexpr *pdrgpexpr,
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	BOOL fConjunction,
	CExpression **ppexprRecheck,
	CExpression **ppexprResidual

	)
{	
	GPOS_ASSERT(NULL != pdrgpexpr); 
		
	const ULONG ulPredicates = pdrgpexpr->UlLength();
	
	if (1 == ulPredicates)
	{
		return PexprBitmap
				(
				pmp, 
				pmda, 
				pexprOriginalPred, 
				(*pdrgpexpr)[0], 
				ptabdesc, 
				pmdrel, 
				pdrgpcrOutput,
				pcrsOuterRefs,
				pcrsReqd, 
				!fConjunction, 
				ppexprRecheck, 
				ppexprResidual
				);
	}
	
	// array of recheck predicates
	DrgPexpr *pdrgpexprRecheckNew = GPOS_NEW(pmp) DrgPexpr(pmp);
	
	// array of residual predicates
	DrgPexpr *pdrgpexprResidualNew = GPOS_NEW(pmp) DrgPexpr(pmp);
			
	// array of bitmap index probe/bitmap bool op expressions
	DrgPexpr *pdrgpexprBitmap = GPOS_NEW(pmp) DrgPexpr(pmp);
	
	CreateBitmapIndexProbeOps
		(
		pmp, 
		pmda, 
		pexprOriginalPred, 
		pdrgpexpr, 
		ptabdesc, 
		pmdrel, 
		pdrgpcrOutput,
		pcrsOuterRefs,
		pcrsReqd, 
		fConjunction, 
		pdrgpexprBitmap, 
		pdrgpexprRecheckNew, 
		pdrgpexprResidualNew
		);

	GPOS_ASSERT(pdrgpexprRecheckNew->UlLength() == pdrgpexprBitmap->UlLength());

	const ULONG ulBitmapExpr = pdrgpexprBitmap->UlLength();

	if (0 == ulBitmapExpr || (!fConjunction && ulBitmapExpr < ulPredicates))
	{
		// no relevant bitmap indexes found,
		// or expression is a disjunction and some disjuncts don't have applicable bitmap indexes
		pdrgpexprBitmap->Release();
		pdrgpexprRecheckNew->Release();
		pdrgpexprResidualNew->Release();
		return NULL;
	}
	
	
	CExpression *pexprBitmapBoolOp = (*pdrgpexprBitmap)[0];
	pexprBitmapBoolOp->AddRef();
	IMDId *pmdidBitmap = CScalar::PopConvert(pexprBitmapBoolOp->Pop())->PmdidType();
	
	for (ULONG ul = 1; ul < ulBitmapExpr; ul++)
	{
		CExpression *pexprBitmap = (*pdrgpexprBitmap)[ul];
		pexprBitmap->AddRef();
		pmdidBitmap->AddRef();

		pexprBitmapBoolOp = PexprBitmapBoolOp(pmp, pmdidBitmap, pexprBitmapBoolOp, pexprBitmap, fConjunction);
	}
	

	GPOS_ASSERT(NULL != pexprBitmapBoolOp && 0 < pdrgpexprRecheckNew->UlLength());
		
	CExpression *pexprRecheckNew = CPredicateUtils::PexprConjDisj(pmp, pdrgpexprRecheckNew, fConjunction);
	if (NULL != *ppexprRecheck)
	{
		CExpression *pexprRecheckNewCombined = CPredicateUtils::PexprConjDisj(pmp, *ppexprRecheck, pexprRecheckNew, fConjunction);
		(*ppexprRecheck)->Release();
		pexprRecheckNew->Release();
		*ppexprRecheck = pexprRecheckNewCombined;
	}
	else 
	{
		*ppexprRecheck = pexprRecheckNew;
	}
	
	if (0 < pdrgpexprResidualNew->UlLength())
	{
		ComputeBitmapTableScanResidualPredicate(pmp, fConjunction, pexprOriginalPred, ppexprResidual, pdrgpexprResidualNew);
	}
	
	pdrgpexprResidualNew->Release();
	
	// cleanup
	pdrgpexprBitmap->Release();	

	return pexprBitmapBoolOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ComputeBitmapTableScanResidualPredicate
//
//	@doc:
//		Compute the residual predicate for a bitmap table scan
//
//---------------------------------------------------------------------------
void
CXformUtils::ComputeBitmapTableScanResidualPredicate
	(
	IMemoryPool *pmp, 
	BOOL fConjunction,
	CExpression *pexprOriginalPred,
	CExpression **ppexprResidual, // input-output argument: the residual predicate computed so-far, and resulting predicate
	DrgPexpr *pdrgpexprResidualNew
	)
{
	GPOS_ASSERT(NULL != pexprOriginalPred);
	GPOS_ASSERT(0 < pdrgpexprResidualNew->UlLength());
	
	if (!fConjunction)
	{
		// one of the disjuncts requires a residual predicate: we need to reevaluate the original predicate
		// for example, for index keys ik1 and ik2, the following will require re-evaluating
		// the whole predicate rather than just k < 100:
		// ik1 = 1 or (ik2=2 and k<100)
		pexprOriginalPred->AddRef();
		CRefCount::SafeRelease(*ppexprResidual);
		*ppexprResidual = pexprOriginalPred;
		return;
	}

	pdrgpexprResidualNew->AddRef();
	CExpression *pexprResidualNew = CPredicateUtils::PexprConjDisj(pmp, pdrgpexprResidualNew, fConjunction);
	
	if (NULL != *ppexprResidual)
	{
		CExpression *pexprResidualNewCombined = CPredicateUtils::PexprConjDisj(pmp, *ppexprResidual, pexprResidualNew, fConjunction);
		(*ppexprResidual)->Release();
		pexprResidualNew->Release();
		*ppexprResidual = pexprResidualNewCombined;	
	}
	else 
	{
		*ppexprResidual = pexprResidualNew;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBitmapBoolOp
//
//	@doc:
//		Construct a bitmap bool op expression between the given bitmap access
// 		path expressions
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBitmapBoolOp
	(
	IMemoryPool *pmp, 
	IMDId *pmdidBitmapType, 
	CExpression *pexprLeft,
	CExpression *pexprRight,
	BOOL fConjunction
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);
	
	CScalarBitmapBoolOp::EBitmapBoolOp ebitmapboolop = CScalarBitmapBoolOp::EbitmapboolAnd;
	
	if (!fConjunction)
	{
		ebitmapboolop = CScalarBitmapBoolOp::EbitmapboolOr;
	}
	
	return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CScalarBitmapBoolOp(pmp, ebitmapboolop, pmdidBitmapType),
				pexprLeft,
				pexprRight
				);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprConditionOnBoolColumn
//
//	@doc:
// 		Creates a condition of the form col = fVal, where col is the given column.
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprEqualityOnBoolColumn
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	BOOL fVal,
	CColRef *pcr
	)
{
	CExpression *pexprConstBool =
			CUtils::PexprScalarConstBool(pmp, fVal, false /*fNull*/);

	const IMDTypeBool *pmdtype = pmda->PtMDType<IMDTypeBool>();
	IMDId *pmdidOp = pmdtype->PmdidCmp(IMDType::EcmptEq);
	pmdidOp->AddRef();

	const CMDName mdname = pmda->Pmdscop(pmdidOp)->Mdname();
	CWStringConst strOpName(mdname.Pstr()->Wsz());

	return CUtils::PexprScalarCmp
					(
					pmp,
					pcr,
					pexprConstBool,
					strOpName,
					pmdidOp
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprConditionOnBoolColumn
//
//	@doc:
//		Construct a bitmap index path expression for the given predicate
//		out of the children of the given expression.
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBitmapFromChildren
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CExpression *pexprOriginalPred,
	CExpression *pexprPred,
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	BOOL fConjunction,
	CExpression **ppexprRecheck,
	CExpression **ppexprResidual
	)
{
	DrgPexpr *pdrgpexpr = NULL;

	if (fConjunction)
	{
		pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprPred);
	}
	else
	{
		pdrgpexpr = CPredicateUtils::PdrgpexprDisjuncts(pmp, pexprPred);
	}

	if (1 == pdrgpexpr->UlLength())
	{
		// unsupported predicate that cannot be split further into conjunctions and disjunctions
		pdrgpexpr->Release();
		return NULL;
	}

	// expression is a deeper tree: recurse further in each of the components
	CExpression *pexprResult = PexprScalarBitmapBoolOp
								(
								pmp,
								pmda,
								pexprOriginalPred,
								pdrgpexpr,
								ptabdesc,
								pmdrel,
								pdrgpcrOutput,
								pcrsOuterRefs,
								pcrsReqd,
								fConjunction,
								ppexprRecheck,
								ppexprResidual
								);
	pdrgpexpr->Release();

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBitmapCondToUse
//
//	@doc:
//		Returns the recheck condition to use in a bitmap index scan computed
//		out of the expression 'pexprPred' that uses the bitmap index.
// 		fBoolColumn (and fNegatedColumn) say whether the predicate is a
// 		(negated) boolean scalar identifier.
// 		Caller takes ownership of the returned expression
//
//---------------------------------------------------------------------------

CExpression *
CXformUtils::PexprBitmapCondToUse
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CExpression *pexprPred,
	BOOL fBoolColumn,
	BOOL fNegatedBoolColumn,
	CColRefSet *pcrsScalar
	)
{
	GPOS_ASSERT(!fBoolColumn || !fNegatedBoolColumn);
	if (fBoolColumn || fNegatedBoolColumn)
	{
		return
			PexprEqualityOnBoolColumn(pmp, pmda, fBoolColumn, pcrsScalar->PcrFirst());
	}
	pexprPred->AddRef();

	return pexprPred;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBitmap
//
//	@doc:
//		Construct a bitmap index path expression for the given predicate
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBitmap
	(
	IMemoryPool *pmp, 
	CMDAccessor *pmda,
	CExpression *pexprOriginalPred,
	CExpression *pexprPred, 
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	BOOL fConjunction,
	CExpression **ppexprRecheck,
	CExpression **ppexprResidual
	)
{	
	GPOS_ASSERT(NULL == *ppexprRecheck);
	GPOS_ASSERT(NULL == *ppexprResidual);

	BOOL fBoolColumn = (COperator::EopScalarIdent == pexprPred->Pop()->Eopid());
	BOOL fNegatedBoolColumn = (!fBoolColumn && CPredicateUtils::FNotIdent(pexprPred));

	// if the expression is not of the form
	// <ident> = <const> or <ident> or NOT <ident>
	// then look for index access paths in its children
	if (!CPredicateUtils::FCompareIdentToConst(pexprPred) &&
		!fBoolColumn && !fNegatedBoolColumn)
	{
		// first, check if it is an index lookup predicate
		CExpression *pexprBitmapForIndexLookup = PexprBitmapForIndexLookup
												(
												pmp,
												pmda,
												pexprPred,
												ptabdesc,
												pmdrel,
												pdrgpcrOutput,
												pcrsOuterRefs,
												pcrsReqd,
												ppexprRecheck
												);
		if (NULL != pexprBitmapForIndexLookup)
		{
			return pexprBitmapForIndexLookup;
		}

		CExpression *pexprBitmapFromChildren = PexprBitmapFromChildren
											(
											pmp,
											pmda,
											pexprOriginalPred,
											pexprPred,
											ptabdesc,
											pmdrel,
											pdrgpcrOutput,
											pcrsOuterRefs,
											pcrsReqd,
											fConjunction,
											ppexprRecheck,
											ppexprResidual
											);
		if (NULL != pexprBitmapFromChildren)
		{
			return pexprBitmapFromChildren;
		}
	}

	// predicate is of the form col op const (or boolean col): find an applicable bitmap index
	return PexprBitmapForSelectCondition
				(
				pmp,
				pmda,
				pexprPred,
				ptabdesc,
				pmdrel,
				pdrgpcrOutput,
				pcrsReqd,
				ppexprRecheck,
				fBoolColumn,
				fNegatedBoolColumn
				);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBitmapForSelectCondition
//
//	@doc:
//		Construct a bitmap index path expression for the given predicate coming
//		from a condition without outer references
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBitmapForSelectCondition
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CExpression *pexprPred,
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsReqd,
	CExpression **ppexprRecheck,
	BOOL fBoolColumn,
	BOOL fNegatedBoolColumn
	)
{
	CColRefSet *pcrsScalar = CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();

	const ULONG ulIndexes = pmdrel->UlIndices();
	for (ULONG ul = 0; ul < ulIndexes; ul++)
	{
		const IMDIndex *pmdindex = pmda->Pmdindex(pmdrel->PmdidIndex(ul));
		
		if (!pmdrel->FPartialIndex(pmdindex->Pmdid()) && CXformUtils::FIndexApplicable
									(
									pmp,
									pmdindex,
									pmdrel,
									pdrgpcrOutput,
									pcrsReqd,
									pcrsScalar,
									IMDIndex::EmdindBitmap
									))
		{
			// found an applicable index
			CExpression *pexprCondToUse = PexprBitmapCondToUse
										(
										pmp,
										pmda,
										pexprPred,
										fBoolColumn,
										fNegatedBoolColumn,
										pcrsScalar
										);
			DrgPexpr *pdrgpexprScalar = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprCondToUse);
			DrgPcr *pdrgpcrIndexCols = PdrgpcrIndexKeys(pmp, pdrgpcrOutput, pmdindex, pmdrel);
			DrgPexpr *pdrgpexprIndex = GPOS_NEW(pmp) DrgPexpr(pmp);
			DrgPexpr *pdrgpexprResidual = GPOS_NEW(pmp) DrgPexpr(pmp);

			CPredicateUtils::ExtractIndexPredicates
				(
				pmp,
				pmda,
				pdrgpexprScalar,
				pmdindex,
				pdrgpcrIndexCols,
				pdrgpexprIndex,
				pdrgpexprResidual,
				NULL  // pcrsAcceptedOuterRefs
				);
			pdrgpexprScalar->Release();
			pdrgpexprResidual->Release();
			if (0 == pdrgpexprIndex->UlLength())
			{
				// no usable predicate, clean up
				pdrgpcrIndexCols->Release();
				pdrgpexprIndex->Release();
				pexprCondToUse->Release();
				continue;
			}

			BOOL fCompatible =
					CPredicateUtils::FCompatiblePredicates(pdrgpexprIndex, pmdindex, pdrgpcrIndexCols, pmda);
			pdrgpcrIndexCols->Release();

			if (!fCompatible)
			{
				pdrgpexprIndex->Release();
				pexprCondToUse->Release();
				continue;
			}

			*ppexprRecheck = pexprCondToUse;
			CIndexDescriptor *pindexdesc = CIndexDescriptor::Pindexdesc(pmp, ptabdesc, pmdindex);
			pmdindex->PmdidItemType()->AddRef();
			return 	GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CScalarBitmapIndexProbe(pmp, pindexdesc, pmdindex->PmdidItemType()),
							pdrgpexprIndex
							);
		}
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBitmapForIndexLookup
//
//	@doc:
//		Construct a bitmap index path expression for the given predicate coming
//		from a condition with outer references that could potentially become
//		an index lookup
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBitmapForIndexLookup
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	CExpression *pexprPred,
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	CExpression **ppexprRecheck
	)
{
	if (NULL == pcrsOuterRefs || 0 == pcrsOuterRefs->CElements())
	{
		return NULL;
	}

	CColRefSet *pcrsScalar = CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();

	const ULONG ulIndexes = pmdrel->UlIndices();
	for (ULONG ul = 0; ul < ulIndexes; ul++)
	{
		const IMDIndex *pmdindex = pmda->Pmdindex(pmdrel->PmdidIndex(ul));

		if (pmdrel->FPartialIndex(pmdindex->Pmdid()) || !CXformUtils::FIndexApplicable
									(
									pmp,
									pmdindex,
									pmdrel,
									pdrgpcrOutput,
									pcrsReqd,
									pcrsScalar,
									IMDIndex::EmdindBitmap
									))
		{
			// skip heterogeneous indexes and indexes that do not match the predicate
			continue;
		}
		DrgPcr *pdrgpcrIndexCols = PdrgpcrIndexKeys(pmp, pdrgpcrOutput, pmdindex, pmdrel);
		// attempt building index lookup predicate
		CExpression *pexprLookupPred = CPredicateUtils::PexprIndexLookup
										(
										pmp,
										pmda,
										pexprPred,
										pmdindex,
										pdrgpcrIndexCols,
										pcrsOuterRefs
										);
		if (NULL != pexprLookupPred &&
				CPredicateUtils::FCompatibleIndexPredicate(pexprLookupPred, pmdindex, pdrgpcrIndexCols, pmda))
		{
			pexprLookupPred->AddRef();
			*ppexprRecheck = pexprLookupPred;
			CIndexDescriptor *pindexdesc = CIndexDescriptor::Pindexdesc(pmp, ptabdesc, pmdindex);
			pmdindex->PmdidItemType()->AddRef();
			pdrgpcrIndexCols->Release();

			return 	GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CScalarBitmapIndexProbe(pmp, pindexdesc, pmdindex->PmdidItemType()),
							pexprLookupPred
							);
		}

		pdrgpcrIndexCols->Release();
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::CreateBitmapIndexProbeOps
//
//	@doc:
//		Given an array of predicate expressions, construct a bitmap access path
//		expression for each predicate and accumulate it in the pdrgpexprBitmap array
//
//---------------------------------------------------------------------------
void
CXformUtils::CreateBitmapIndexProbeOps
	(
	IMemoryPool *pmp, 
	CMDAccessor *pmda,
	CExpression *pexprOriginalPred,
	DrgPexpr *pdrgpexpr, 
	CTableDescriptor *ptabdesc,
	const IMDRelation *pmdrel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd,
	BOOL fConjunction,
	DrgPexpr *pdrgpexprBitmap,
	DrgPexpr *pdrgpexprRecheck,
	DrgPexpr *pdrgpexprResidual
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	
	const ULONG ulPredicates = pdrgpexpr->UlLength();

	for (ULONG ul = 0; ul < ulPredicates; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		CExpression *pexprRecheck = NULL;
		CExpression *pexprResidual = NULL;
		CExpression *pexprBitmap = CXformUtils::PexprBitmap
									(
									pmp,
									pmda,
									pexprOriginalPred,
									pexprPred,
									ptabdesc,
									pmdrel,
									pdrgpcrOutput,
									pcrsOuterRefs,
									pcrsReqd,
									!fConjunction,
									&pexprRecheck,
									&pexprResidual
									);
		if (NULL != pexprBitmap)
		{
			GPOS_ASSERT(NULL != pexprRecheck);

			// if possible, merge this index scan with a previous index scan on the same index
			BOOL fAddedToPredicate = fConjunction && FMergeWithPreviousBitmapIndexProbe
													(
													pmp,
													pexprBitmap,
													pexprRecheck,
													pdrgpexprBitmap,
													pdrgpexprRecheck
													);
			if (NULL != pexprResidual)
			{
				pdrgpexprResidual->Append(pexprResidual);
			}
			if (fAddedToPredicate)
			{
				pexprBitmap->Release();
				pexprRecheck->Release();
			}
			else
			{
				pdrgpexprRecheck->Append(pexprRecheck);
				pdrgpexprBitmap->Append(pexprBitmap);
			}
		}
		else
		{
			pexprPred->AddRef();
			pdrgpexprResidual->Append(pexprPred);
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FHasAmbiguousType
//
//	@doc:
//		Check if expression has a scalar node with ambiguous return type
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FHasAmbiguousType
	(
	CExpression *pexpr,
	CMDAccessor *pmda
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pmda);

	BOOL fAmbiguous = false;
	if (pexpr->Pop()->FScalar())
	{
		CScalar *popScalar = CScalar::PopConvert(pexpr->Pop());
		switch (popScalar->Eopid())
		{
			case COperator::EopScalarAggFunc:
				fAmbiguous = CScalarAggFunc::PopConvert(popScalar)->FHasAmbiguousReturnType();
				break;

			case COperator::EopScalarProjectList:
			case COperator::EopScalarProjectElement:
			case COperator::EopScalarSwitchCase:
				break; // these operators do not have valid return type

			default:
				IMDId *pmdid = popScalar->PmdidType();
				if (NULL != pmdid)
				{
					// check MD type of scalar node
					fAmbiguous = pmda->Pmdtype(pmdid)->FAmbiguous();
				}
		}
	}

	if (!fAmbiguous)
	{
		// recursively process children
		const ULONG ulArity = pexpr->UlArity();
		for (ULONG ul = 0; !fAmbiguous && ul < ulArity; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];
			fAmbiguous = FHasAmbiguousType(pexprChild, pmda);
		}
	}

	return fAmbiguous;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprSelect2BitmapBoolOp
//
//	@doc:
//		Transform a Select into a Bitmap(Dynamic)TableGet over BitmapBoolOp if
//		bitmap indexes exist
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprSelect2BitmapBoolOp
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	CLogical *popGet = CLogical::PopConvert(pexprRelational->Pop());

	CTableDescriptor *ptabdesc = CLogical::PtabdescFromTableGet(popGet);
	const ULONG ulIndices = ptabdesc->UlIndices();
	if (0 == ulIndices)
	{
		return NULL;
	}

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsScalarExpr = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();

	CColRefSet *pcrsReqd = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsReqd->Include(pcrsOutput);
	pcrsReqd->Include(pcrsScalarExpr);

	CExpression *pexprResult = PexprBitmapTableGet
								(
								pmp,
								popGet,
								pexpr->Pop()->UlOpId(),
								ptabdesc,
								pexprScalar,
								NULL,  // pcrsOuterRefs
								pcrsReqd
								);
	pcrsReqd->Release();

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprBitmapTableGet
//
//	@doc:
//		Transform a Select into a Bitmap(Dynamic)TableGet over BitmapBoolOp if
//		bitmap indexes exist
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprBitmapTableGet
	(
	IMemoryPool *pmp,
	CLogical *popGet,
	ULONG ulOriginOpId,
	CTableDescriptor *ptabdesc,
	CExpression *pexprScalar,
	CColRefSet *pcrsOuterRefs,
	CColRefSet *pcrsReqd
	)
{
	GPOS_ASSERT(COperator::EopLogicalGet == popGet->Eopid() || 
			COperator::EopLogicalDynamicGet == popGet->Eopid());
	
	BOOL fDynamicGet = (COperator::EopLogicalDynamicGet == popGet->Eopid());
	
	// array of expressions in the scalar expression
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	GPOS_ASSERT(0 < pdrgpexpr->UlLength());

	// find the indexes whose included columns meet the required columns
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());

	GPOS_ASSERT(0 < pdrgpexpr->UlLength());
	
	DrgPcr *pdrgpcrOutput = CLogical::PdrgpcrOutputFromLogicalGet(popGet);
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	
	CExpression *pexprRecheck = NULL;
	CExpression *pexprResidual = NULL;
	CExpression *pexprBitmap = PexprScalarBitmapBoolOp
				(
				pmp,
				pmda,
				pexprScalar,
				pdrgpexpr,
				ptabdesc,
				pmdrel,
				pdrgpcrOutput,
				pcrsOuterRefs,
				pcrsReqd,
				true, // fConjunction
				&pexprRecheck,
				&pexprResidual
				);
	CExpression *pexprResult = NULL;

	if (NULL != pexprBitmap)
	{
		GPOS_ASSERT(NULL != pexprRecheck);
		ptabdesc->AddRef();
		pdrgpcrOutput->AddRef();
		
		CName *pname = 	GPOS_NEW(pmp) CName(pmp, CName(CLogical::NameFromLogicalGet(popGet).Pstr()));

		// create a bitmap table scan on top
		CLogical *popBitmapTableGet = NULL;
		
		if (fDynamicGet)
		{
			CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(popGet);
			CPartConstraint *ppartcnstr = popDynamicGet->Ppartcnstr();
			ppartcnstr->AddRef();
			ppartcnstr->AddRef();
			popDynamicGet->PdrgpdrgpcrPart()->AddRef();
			popBitmapTableGet = GPOS_NEW(pmp) CLogicalDynamicBitmapTableGet
								(
								pmp, 
								ptabdesc, 
								ulOriginOpId,
								pname, 
								popDynamicGet->UlScanId(), 
								pdrgpcrOutput,
								popDynamicGet->PdrgpdrgpcrPart(),
								popDynamicGet->UlSecondaryScanId(),
								false, // fPartial
								ppartcnstr,
								ppartcnstr
								);
		}
		else
		{
			popBitmapTableGet = GPOS_NEW(pmp) CLogicalBitmapTableGet(pmp, ptabdesc, ulOriginOpId, pname, pdrgpcrOutput);
		}
		pexprResult = GPOS_NEW(pmp) CExpression
						(
						pmp,
						popBitmapTableGet,
						pexprRecheck,
						pexprBitmap
						);
		
		if (NULL != pexprResidual)
		{
			// add a selection on top with the residual condition
			pexprResult = GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CLogicalSelect(pmp),
							pexprResult,
							pexprResidual
							);				
		}
	}
	
	// cleanup
	pdrgpexpr->Release();

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpdrgppartdigCandidates
//
//	@doc:
//		Find a set of partial index combinations
//
//---------------------------------------------------------------------------
DrgPdrgPpartdig *
CXformUtils::PdrgpdrgppartdigCandidates
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPexpr *pdrgpexprScalar,
	DrgDrgPcr *pdrgpdrgpcrPartKey,
	const IMDRelation *pmdrel,
	CPartConstraint *ppartcnstrRel,
	DrgPcr *pdrgpcrOutput,
	CColRefSet *pcrsReqd,
	CColRefSet *pcrsScalarExpr,
	CColRefSet *pcrsAcceptedOuterRefs
	)
{
	DrgPdrgPpartdig *pdrgpdrgppartdig = GPOS_NEW(pmp) DrgPdrgPpartdig(pmp);
	const ULONG ulIndexes = pmdrel->UlIndices();

	// currently covered parts
	CPartConstraint *ppartcnstrCovered = NULL;
	DrgPpartdig *pdrgppartdig = GPOS_NEW(pmp) DrgPpartdig(pmp);

	for (ULONG ul = 0; ul < ulIndexes; ul++)
	{
		const IMDIndex *pmdindex = pmda->Pmdindex(pmdrel->PmdidIndex(ul));

		if (!CXformUtils::FIndexApplicable(pmp, pmdindex, pmdrel, pdrgpcrOutput, pcrsReqd, pcrsScalarExpr, IMDIndex::EmdindBtree /*emdindtype*/) ||
			!pmdrel->FPartialIndex(pmdindex->Pmdid()))
		{
			// not a partial index (handled in another function), or index does not apply to predicate
			continue;
		}
		
		CPartConstraint *ppartcnstr = CUtils::PpartcnstrFromMDPartCnstr(pmp, pmda, pdrgpdrgpcrPartKey, pmdindex->Pmdpartcnstr(), pdrgpcrOutput);
		DrgPexpr *pdrgpexprIndex = GPOS_NEW(pmp) DrgPexpr(pmp);
		DrgPexpr *pdrgpexprResidual = GPOS_NEW(pmp) DrgPexpr(pmp);
		CPartConstraint *ppartcnstrNewlyCovered = PpartcnstrUpdateCovered
														(
														pmp,
														pmda,
														pdrgpexprScalar,
														ppartcnstrCovered,
														ppartcnstr,
														pdrgpcrOutput,
														pdrgpexprIndex,
														pdrgpexprResidual,
														pmdrel,
														pmdindex,
														pcrsAcceptedOuterRefs
														);

		if (NULL == ppartcnstrNewlyCovered)
		{
			ppartcnstr->Release();
			pdrgpexprResidual->Release();
			pdrgpexprIndex->Release();
			continue;
		}

		CRefCount::SafeRelease(ppartcnstrCovered);
		ppartcnstrCovered = ppartcnstrNewlyCovered;
		
		pdrgppartdig->Append(GPOS_NEW(pmp) SPartDynamicIndexGetInfo(pmdindex, ppartcnstr, pdrgpexprIndex, pdrgpexprResidual));
	}

	if (NULL != ppartcnstrCovered && !ppartcnstrRel->FEquivalent(ppartcnstrCovered))
	{
		pdrgpexprScalar->AddRef();
		SPartDynamicIndexGetInfo *ppartdig = PpartdigDynamicGet(pmp, pdrgpexprScalar, ppartcnstrCovered, ppartcnstrRel);
		if (NULL == ppartdig)
		{
			CRefCount::SafeRelease(ppartcnstrCovered);
			pdrgppartdig->Release();
			return pdrgpdrgppartdig;
		}

		pdrgppartdig->Append(ppartdig);
	}

	CRefCount::SafeRelease(ppartcnstrCovered);

	pdrgpdrgppartdig->Append(pdrgppartdig);
	return pdrgpdrgppartdig;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PpartcnstrUpdateCovered
//
//	@doc:
//		Compute the newly covered part constraint based on the old covered part
//		constraint and the given part constraint
//
//---------------------------------------------------------------------------
CPartConstraint *
CXformUtils::PpartcnstrUpdateCovered
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPexpr *pdrgpexprScalar,
	CPartConstraint *ppartcnstrCovered,
	CPartConstraint *ppartcnstr,
	DrgPcr *pdrgpcrOutput,
	DrgPexpr *pdrgpexprIndex,
	DrgPexpr *pdrgpexprResidual,
	const IMDRelation *pmdrel,
	const IMDIndex *pmdindex,
	CColRefSet *pcrsAcceptedOuterRefs
	)
{
	if (NULL == ppartcnstr->PcnstrCombined())
	{
		// unsupported constraint type: do not produce a partial index scan as we cannot reason about it
		return NULL;
	}

	if (NULL != ppartcnstrCovered && ppartcnstrCovered->FOverlap(pmp, ppartcnstr))
	{
		// index overlaps with already considered indexes: skip
		return NULL;
	}

	DrgPcr *pdrgpcrIndexCols = PdrgpcrIndexKeys(pmp, pdrgpcrOutput, pmdindex, pmdrel);
	CPredicateUtils::ExtractIndexPredicates
						(
						pmp,
						pmda,
						pdrgpexprScalar,
						pmdindex,
						pdrgpcrIndexCols,
						pdrgpexprIndex,
						pdrgpexprResidual,
						pcrsAcceptedOuterRefs
						);

	pdrgpcrIndexCols->Release();
	if (0 == pdrgpexprIndex->UlLength())
	{
		// no predicate could use the index: clean up
		return NULL;
	}

	return CXformUtils::PpartcnstrDisjunction(pmp, ppartcnstrCovered, ppartcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PpartcnstrDisjunction
//
//	@doc:
//		Compute a disjunction of two part constraints
//
//---------------------------------------------------------------------------
CPartConstraint *
CXformUtils::PpartcnstrDisjunction
	(
	IMemoryPool *pmp,
	CPartConstraint *ppartcnstrOld,
	CPartConstraint *ppartcnstrNew
	)
{
	GPOS_ASSERT(NULL != ppartcnstrNew);
	
	if (NULL == ppartcnstrOld)
	{
		ppartcnstrNew->AddRef();
		return ppartcnstrNew;
	}

	return CPartConstraint::PpartcnstrDisjunction(pmp, ppartcnstrOld, ppartcnstrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PpartdigDynamicGet
//
//	@doc:
//		Create a dynamic table get candidate to cover the partitions not covered
//		by the partial index scans
//
//---------------------------------------------------------------------------
SPartDynamicIndexGetInfo *
CXformUtils::PpartdigDynamicGet
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexprScalar,
	CPartConstraint *ppartcnstrCovered,
	CPartConstraint *ppartcnstrRel
	)
{
	GPOS_ASSERT(!ppartcnstrCovered->FUnbounded());
	CPartConstraint *ppartcnstrRest = ppartcnstrRel->PpartcnstrRemaining(pmp, ppartcnstrCovered);
	if (NULL == ppartcnstrRest)
	{
		return NULL;
	}

	return GPOS_NEW(pmp) SPartDynamicIndexGetInfo(NULL /*pmdindex*/, ppartcnstrRest, NULL /* pdrgpexprIndex */, pdrgpexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprRemapColumns
//
//	@doc:
//		Remap the expression from the old columns to the new ones
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprRemapColumns
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPcr *pdrgpcrA,
	DrgPcr *pdrgpcrRemappedA,
	DrgPcr *pdrgpcrB,
	DrgPcr *pdrgpcrRemappedB
	)
{
	HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, pdrgpcrA, pdrgpcrRemappedA);
	GPOS_ASSERT_IMP(NULL == pdrgpcrB, NULL == pdrgpcrRemappedB);
	if (NULL != pdrgpcrB)
	{
		CUtils::AddColumnMapping(pmp, phmulcr, pdrgpcrB, pdrgpcrRemappedB);
	}
	CExpression *pexprRemapped = pexpr->PexprCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
	phmulcr->Release();

	return pexprRemapped;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprPartialDynamicIndexGet
//
//	@doc:
//		Create a dynamic index get plan for the given partial index
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprPartialDynamicIndexGet
	(
	IMemoryPool *pmp,
	CLogicalDynamicGet *popGet,
	ULONG ulOriginOpId,
	DrgPexpr *pdrgpexprIndex,
	DrgPexpr *pdrgpexprResidual,
	DrgPcr *pdrgpcrDIG,
	const IMDIndex *pmdindex,
	const IMDRelation *pmdrel,
	CPartConstraint *ppartcnstr,
	CColRefSet *pcrsAcceptedOuterRefs,
	DrgPcr *pdrgpcrOuter,
	DrgPcr *pdrgpcrNewOuter
	)
{
	GPOS_ASSERT_IMP(NULL == pdrgpcrOuter, NULL == pcrsAcceptedOuterRefs);
	GPOS_ASSERT_IMP(NULL != pdrgpcrOuter, NULL != pdrgpcrNewOuter);
	GPOS_ASSERT(NULL != pmdindex);
	GPOS_ASSERT(pmdrel->FPartialIndex(pmdindex->Pmdid()));

	DrgPcr *pdrgpcrIndexCols = PdrgpcrIndexKeys(pmp, popGet->PdrgpcrOutput(), pmdindex, pmdrel);

	HMUlCr *phmulcr = NULL;

	if (popGet->PdrgpcrOutput() != pdrgpcrDIG)
	{
		// columns need to be remapped
		phmulcr = CUtils::PhmulcrMapping(pmp, popGet->PdrgpcrOutput(), pdrgpcrDIG);
	}

	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	ptabdesc->AddRef();

	CWStringConst strTableAliasName(pmp, popGet->Name().Pstr()->Wsz());

	DrgDrgPcr *pdrgpdrgpcrPart = NULL;
	CPartConstraint *ppartcnstrDIG = NULL;
	DrgPexpr *pdrgpexprIndexRemapped = NULL;
	DrgPexpr *pdrgpexprResidualRemapped = NULL;
	CPartConstraint *ppartcnstrRel = NULL;

	if (NULL != phmulcr)
	{
		// if there are any outer references, add them to the mapping
		if (NULL != pcrsAcceptedOuterRefs)
		{
			ULONG ulOuterPcrs = pdrgpcrOuter->UlLength();
			GPOS_ASSERT(ulOuterPcrs == pdrgpcrNewOuter->UlLength());

			for (ULONG ul = 0; ul < ulOuterPcrs; ul++)
			{
				CColRef *pcrOld = (*pdrgpcrOuter)[ul];
				CColRef *pcrNew = (*pdrgpcrNewOuter)[ul];
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif
				phmulcr->FInsert(GPOS_NEW(pmp) ULONG(pcrOld->UlId()), pcrNew);
				GPOS_ASSERT(fInserted);
			}
		}

		pdrgpdrgpcrPart = CUtils::PdrgpdrgpcrRemap(pmp, popGet->PdrgpdrgpcrPart(), phmulcr, true /*fMustExist*/);
		ppartcnstrDIG = ppartcnstr->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
		ppartcnstrRel = popGet->PpartcnstrRel()->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);

		pdrgpexprIndexRemapped = CUtils::PdrgpexprRemap(pmp, pdrgpexprIndex, phmulcr);
		pdrgpexprResidualRemapped = CUtils::PdrgpexprRemap(pmp, pdrgpexprResidual, phmulcr);
	}
	else
	{
		popGet->PdrgpdrgpcrPart()->AddRef();
		ppartcnstr->AddRef();
		pdrgpexprIndex->AddRef();
		pdrgpexprResidual->AddRef();
		popGet->PpartcnstrRel()->AddRef();

		pdrgpdrgpcrPart = popGet->PdrgpdrgpcrPart();
		ppartcnstrDIG = ppartcnstr;
		pdrgpexprIndexRemapped = pdrgpexprIndex;
		pdrgpexprResidualRemapped = pdrgpexprResidual;
		ppartcnstrRel = popGet->PpartcnstrRel();
	}
	pdrgpcrDIG->AddRef();

	// create the logical index get operator
	CLogicalDynamicIndexGet *popIndexGet = GPOS_NEW(pmp) CLogicalDynamicIndexGet
												(
												pmp,
												pmdindex,
												ptabdesc,
												ulOriginOpId,
												GPOS_NEW(pmp) CName(pmp, CName(&strTableAliasName)),
												popGet->UlScanId(),
												pdrgpcrDIG,
												pdrgpdrgpcrPart,
												COptCtxt::PoctxtFromTLS()->UlPartIndexNextVal(),
												ppartcnstrDIG,
												ppartcnstrRel
												);


	CExpression *pexprIndexCond = CPredicateUtils::PexprConjunction(pmp, pdrgpexprIndexRemapped);
	CExpression *pexprResidualCond = CPredicateUtils::PexprConjunction(pmp, pdrgpexprResidualRemapped);

	// cleanup
	CRefCount::SafeRelease(phmulcr);
	pdrgpcrIndexCols->Release();

	// create the expression containing the logical index get operator
	return CUtils::PexprSafeSelect(pmp, GPOS_NEW(pmp) CExpression(pmp, popIndexGet, pexprIndexCond), pexprResidualCond);
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FJoinPredOnSingleChild
//
//	@doc:
//		Check if expression handle is attached to a Join expression
//		with a join predicate that uses columns from a single child
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FJoinPredOnSingleChild
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(CUtils::FLogicalJoin(exprhdl.Pop()));

	const ULONG ulArity = exprhdl.UlArity();
	if (0 == exprhdl.Pdpscalar(ulArity - 1)->PcrsUsed()->CElements())
	{
		// no columns are used in join predicate
		return false;
	}

	// construct array of children output columns
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	for (ULONG ul = 0; ul < ulArity - 1; ul++)
	{
		CColRefSet *pcrsOutput = exprhdl.Pdprel(ul)->PcrsOutput();
		pcrsOutput->AddRef();
		pdrgpcrs->Append(pcrsOutput);
	}

	DrgPexpr *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(pmp, exprhdl.PexprScalarChild(ulArity- 1));
	const ULONG ulPreds = pdrgpexprPreds->UlLength();
	BOOL fPredUsesSingleChild = false;
	for (ULONG ulPred = 0; !fPredUsesSingleChild && ulPred < ulPreds; ulPred++)
	{
		CExpression *pexpr = (*pdrgpexprPreds)[ulPred];
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed();
		for (ULONG ulChild = 0; !fPredUsesSingleChild && ulChild < ulArity - 1; ulChild++)
		{
			fPredUsesSingleChild = (*pdrgpcrs)[ulChild]->FSubset(pcrsUsed);
		}
	}
	pdrgpexprPreds->Release();
	pdrgpcrs->Release();

	return fPredUsesSingleChild;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprCTEConsumer
//
//	@doc:
//		Create a new CTE consumer for the given CTE id
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprCTEConsumer
	(
	IMemoryPool *pmp,
	ULONG ulCTEId,
	DrgPcr *pdrgpcr
	)
{
	CLogicalCTEConsumer *popConsumer = GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcr);
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->IncrementConsumers(ulCTEId);

	return GPOS_NEW(pmp) CExpression(pmp, popConsumer);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpcrSubsequence
//
//	@doc:
//		Returns a new array containing the columns from the given column array 'pdrgpcr'
//		at the positions indicated by the given ULONG array 'pdrgpulIndexesOfRefs'
//
//---------------------------------------------------------------------------
DrgPcr *
CXformUtils::PdrgpcrReorderedSubsequence
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	DrgPul *pdrgpulIndexesOfRefs
	)
{
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(NULL != pdrgpulIndexesOfRefs);

	const ULONG ulLength = pdrgpulIndexesOfRefs->UlLength();
	GPOS_ASSERT(ulLength <= pdrgpcr->UlLength());

	DrgPcr *pdrgpcrNewSubsequence = GPOS_NEW(pmp) DrgPcr(pmp);
	for (ULONG ul = 0; ul < ulLength; ul++)
	{
		ULONG ulPos = *(*pdrgpulIndexesOfRefs)[ul];
		GPOS_ASSERT(ulPos < pdrgpcr->UlLength());
		pdrgpcrNewSubsequence->Append((*pdrgpcr)[ulPos]);
	}

	return pdrgpcrNewSubsequence;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::FMergeWithPreviousBitmapIndexProbe
//
//	@doc:
//		If there is already an index probe in pdrgpexprBitmap on the same
//		index as the given pexprBitmap, modify the existing index probe and
//		the corresponding recheck conditions to subsume pexprBitmap and
//		pexprRecheck respectively
//
//---------------------------------------------------------------------------
BOOL
CXformUtils::FMergeWithPreviousBitmapIndexProbe
	(
	IMemoryPool *pmp,
	CExpression *pexprBitmap,
	CExpression *pexprRecheck,
	DrgPexpr *pdrgpexprBitmap,
	DrgPexpr *pdrgpexprRecheck
	)
{
	if (COperator::EopScalarBitmapIndexProbe != pexprBitmap->Pop()->Eopid())
	{
		return false;
	}

	CScalarBitmapIndexProbe *popScalar = CScalarBitmapIndexProbe::PopConvert(pexprBitmap->Pop());
	IMDId *pmdIdIndex = popScalar->Pindexdesc()->Pmdid();
	const ULONG ulNumScalars = pdrgpexprBitmap->UlLength();
	for (ULONG ul = 0; ul < ulNumScalars; ul++)
	{
		CExpression *pexpr = (*pdrgpexprBitmap)[ul];
		COperator *pop = pexpr->Pop();
		if (COperator::EopScalarBitmapIndexProbe != pop->Eopid())
		{
			continue;
		}
		CScalarBitmapIndexProbe *popScalarBIP = CScalarBitmapIndexProbe::PopConvert(pop);
		if (!popScalarBIP->Pindexdesc()->Pmdid()->FEquals(pmdIdIndex))
		{
			continue;
		}

		// create a new expression with the merged conjuncts and
		// replace the old expression in the expression array
		CExpression *pexprNew = CPredicateUtils::PexprConjunction(pmp, (*pexpr)[0], (*pexprBitmap)[0]);
		popScalarBIP->AddRef();
		CExpression *pexprBitmapNew = GPOS_NEW(pmp) CExpression(pmp, popScalarBIP, pexprNew);
		pdrgpexprBitmap->Replace(ul, pexprBitmapNew);

		CExpression *pexprRecheckNew =
				CPredicateUtils::PexprConjunction(pmp, (*pdrgpexprRecheck)[ul], pexprRecheck);
		pdrgpexprRecheck->Replace(ul, pexprRecheckNew);

		return true;
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprWinFuncAgg2ScalarAgg
//
//	@doc:
//		Converts an Agg window function into regular Agg
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprWinFuncAgg2ScalarAgg
	(
	IMemoryPool *pmp,
	CExpression *pexprWinFunc
	)
{
	GPOS_ASSERT(NULL != pexprWinFunc);
	GPOS_ASSERT(COperator::EopScalarWindowFunc == pexprWinFunc->Pop()->Eopid());

	DrgPexpr *pdrgpexprWinFuncArgs = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArgs = pexprWinFunc->UlArity();
	for (ULONG ul = 0; ul < ulArgs; ul++)
	{
		CExpression *pexprArg = (*pexprWinFunc)[ul];
		pexprArg->AddRef();
		pdrgpexprWinFuncArgs->Append(pexprArg);
	}

	CScalarWindowFunc *popScWinFunc = CScalarWindowFunc::PopConvert(pexprWinFunc->Pop());
	IMDId *pmdidFunc = popScWinFunc->PmdidFunc();

	pmdidFunc->AddRef();
	return
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			CUtils::PopAggFunc
				(
				pmp,
				pmdidFunc,
				GPOS_NEW(pmp) CWStringConst(pmp, popScWinFunc->PstrFunc()->Wsz()),
				popScWinFunc->FDistinct(),
				EaggfuncstageGlobal,
				false // fSplit
				),
			pdrgpexprWinFuncArgs
		);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::MapPrjElemsWithDistinctAggs
//
//	@doc:
//		Given a project list, create a map whose key is the argument of
//		distinct Agg, and value is the set of project elements that define
//		distinct Aggs on that argument,
//		non-distinct Aggs are grouped together in one set with key 'True',
//		for example,
//
//		Input: (x : count(distinct a),
//				y : sum(distinct a),
//				z : avg(distinct b),
//				w : max(c))
//
//		Output: (a --> {x,y},
//				 b --> {z},
//				 true --> {w})
//
//---------------------------------------------------------------------------
void
CXformUtils::MapPrjElemsWithDistinctAggs
	(
	IMemoryPool *pmp,
	CExpression *pexprPrjList,
	HMExprDrgPexpr **pphmexprdrgpexpr, // output: created map
	ULONG *pulDifferentDQAs // output: number of DQAs with different arguments
	)
{
	GPOS_ASSERT(NULL != pexprPrjList);
	GPOS_ASSERT(NULL != pphmexprdrgpexpr);
	GPOS_ASSERT(NULL != pulDifferentDQAs);

	HMExprDrgPexpr *phmexprdrgpexpr = GPOS_NEW(pmp) HMExprDrgPexpr(pmp);
	ULONG ulDifferentDQAs = 0;
	CExpression *pexprTrue = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
	const ULONG ulArity = pexprPrjList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrjEl = (*pexprPrjList)[ul];
		CExpression *pexprChild = (*pexprPrjEl)[0];
		COperator *popChild = pexprChild->Pop();
		COperator::EOperatorId eopidChild = popChild->Eopid();
		if (COperator::EopScalarAggFunc != eopidChild &&
			COperator::EopScalarWindowFunc != eopidChild)
		{
			continue;
		}

		BOOL fDistinct = false;
		if (COperator::EopScalarAggFunc == eopidChild)
		{
			fDistinct = CScalarAggFunc::PopConvert(popChild)->FDistinct();
		}
		else
		{
			fDistinct = CScalarWindowFunc::PopConvert(popChild)->FDistinct();
		}

		CExpression *pexprKey = NULL;
		if (fDistinct && 1 == pexprChild->UlArity())
		{
			// use first argument of Distinct Agg as key
			pexprKey = (*pexprChild)[0];
		}
		else
		{
			// use constant True as key
			pexprKey = pexprTrue;
		}

		DrgPexpr *pdrgpexpr = const_cast<DrgPexpr *>(phmexprdrgpexpr->PtLookup(pexprKey));
		BOOL fExists = (NULL != pdrgpexpr);
		if (!fExists)
		{
			// first occurrence, create a new expression array
			pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		}
		pexprPrjEl->AddRef();
		pdrgpexpr->Append(pexprPrjEl);

		if (!fExists)
		{
			pexprKey->AddRef();
#ifdef 	GPOS_DEBUG
			BOOL fSuccess =
#endif // GPOS_DEBUG
				phmexprdrgpexpr->FInsert(pexprKey, pdrgpexpr);
			GPOS_ASSERT(fSuccess);

			if (pexprKey != pexprTrue)
			{
				// first occurrence of a new DQA, increment counter
				ulDifferentDQAs++;
			}
		}
	}

	pexprTrue->Release();

	*pphmexprdrgpexpr = phmexprdrgpexpr;
	*pulDifferentDQAs = ulDifferentDQAs;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::ICmpPrjElemsArr
//
//	@doc:
//		Comparator used in sorting arrays of project elements
//		based on the column id of the first entry
//
//---------------------------------------------------------------------------
INT
CXformUtils::ICmpPrjElemsArr
	(
	const void *pvFst,
	const void *pvSnd
	)
{
	GPOS_ASSERT(NULL != pvFst);
	GPOS_ASSERT(NULL != pvSnd);

	const DrgPexpr *pdrgpexprFst = *(const DrgPexpr **) (pvFst);
	const DrgPexpr *pdrgpexprSnd = *(const DrgPexpr **) (pvSnd);

	CExpression *pexprPrjElemFst = (*pdrgpexprFst)[0];
	CExpression *pexprPrjElemSnd = (*pdrgpexprSnd)[0];
	ULONG ulIdFst = CScalarProjectElement::PopConvert(pexprPrjElemFst->Pop())->Pcr()->UlId();
	ULONG ulIdSnd = CScalarProjectElement::PopConvert(pexprPrjElemSnd->Pop())->Pcr()->UlId();

	if (ulIdFst < ulIdSnd)
	{
		return -1;
	}

	if (ulIdFst > ulIdSnd)
	{
		return 1;
	}

	return 0;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PdrgpdrgpexprSortedPrjElemsArray
//
//	@doc:
//		Iterate over given hash map and return array of arrays of project
//		elements sorted by the column id of the first entries
//
//---------------------------------------------------------------------------
DrgPdrgPexpr *
CXformUtils::PdrgpdrgpexprSortedPrjElemsArray
	(
	IMemoryPool *pmp,
	HMExprDrgPexpr *phmexprdrgpexpr
	)
{
	GPOS_ASSERT(NULL != phmexprdrgpexpr);

	DrgPdrgPexpr *pdrgpdrgpexprPrjElems = GPOS_NEW(pmp) DrgPdrgPexpr(pmp);
	HMExprDrgPexprIter hmexprdrgpexpriter(phmexprdrgpexpr);
	while (hmexprdrgpexpriter.FAdvance())
	{
		DrgPexpr *pdrgpexprPrjElems = const_cast<DrgPexpr *>(hmexprdrgpexpriter.Pt());
		pdrgpexprPrjElems->AddRef();
		pdrgpdrgpexprPrjElems->Append(pdrgpexprPrjElems);
	}
	pdrgpdrgpexprPrjElems->Sort(ICmpPrjElemsArr);

	return pdrgpdrgpexprPrjElems;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUtils::PexprGbAggOnCTEConsumer2Join
//
//	@doc:
//		Convert GbAgg with distinct aggregates to a join expression
//
//		each leaf node of the resulting join expression is a GbAgg on a single
//		distinct aggs, we also create a GbAgg leaf for all remaining (non-distinct)
//		aggregates, for example
//
//		Input:
//			GbAgg_(count(distinct a), max(distinct a), sum(distinct b), avg(a))
//				+---CTEConsumer
//
//		Output:
//			InnerJoin
//				|--InnerJoin
//				|		|--GbAgg_(count(distinct a), max(distinct a))
//				|		|		+---CTEConsumer
//				|		+--GbAgg_(sum(distinct b))
//				|				+---CTEConsumer
//				+--GbAgg_(avg(a))
//						+---CTEConsumer
//
//---------------------------------------------------------------------------
CExpression *
CXformUtils::PexprGbAggOnCTEConsumer2Join
	(
	IMemoryPool *pmp,
	CExpression *pexprGbAgg
	)
{
	GPOS_ASSERT(NULL != pexprGbAgg);
	GPOS_ASSERT(COperator::EopLogicalGbAgg == pexprGbAgg->Pop()->Eopid());

	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(pexprGbAgg->Pop());
	DrgPcr *pdrgpcrGrpCols = popGbAgg->Pdrgpcr();

	GPOS_ASSERT(popGbAgg->FGlobal());

	if (COperator::EopLogicalCTEConsumer != (*pexprGbAgg)[0]->Pop()->Eopid())
	{
		// child of GbAgg must be a CTE consumer

		return NULL;
	}

	CExpression *pexprPrjList = (*pexprGbAgg)[1];
	ULONG ulDistinctAggs = CDrvdPropScalar::Pdpscalar(pexprPrjList->PdpDerive())->UlDistinctAggs();

	if (1 == ulDistinctAggs)
	{
		// if only one distinct agg is used, return input expression
		pexprGbAgg->AddRef();
		return pexprGbAgg;
	}

	HMExprDrgPexpr *phmexprdrgpexpr = NULL;
	ULONG ulDifferentDQAs = 0;
	MapPrjElemsWithDistinctAggs(pmp, pexprPrjList, &phmexprdrgpexpr, &ulDifferentDQAs);
	if (1 == phmexprdrgpexpr->UlEntries())
	{
		// if all distinct aggs use the same argument, return input expression
		phmexprdrgpexpr->Release();

		pexprGbAgg->AddRef();
		return pexprGbAgg;
	}

	CExpression *pexprCTEConsumer = (*pexprGbAgg)[0];
	CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pexprCTEConsumer->Pop());
	const ULONG ulCTEId = popConsumer->UlCTEId();
	DrgPcr *pdrgpcrConsumerOutput = popConsumer->Pdrgpcr();
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();

	CExpression *pexprLastGbAgg = NULL;
	DrgPcr *pdrgpcrLastGrpCols = NULL;
	CExpression *pexprJoin = NULL;
	CExpression *pexprTrue = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);

	// iterate over map to extract sorted array of array of project elements,
	// we need to sort arrays here since hash map iteration is non-deterministic,
	// which may create non-deterministic ordering of join children leading to
	// changing the plan of the same query when run multiple times
	DrgPdrgPexpr *pdrgpdrgpexprPrjElems = PdrgpdrgpexprSortedPrjElemsArray(pmp, phmexprdrgpexpr);

	// counter of consumers
	ULONG ulConsumers = 0;

	const ULONG ulSize = pdrgpdrgpexprPrjElems->UlLength();
	for (ULONG ulPrjElemsArr = 0; ulPrjElemsArr < ulSize; ulPrjElemsArr++)
	{
		DrgPexpr *pdrgpexprPrjElems = (*pdrgpdrgpexprPrjElems)[ulPrjElemsArr];

		CExpression *pexprNewGbAgg = NULL;
		if (0 == ulConsumers)
		{
			// reuse input consumer
			pdrgpcrGrpCols->AddRef();
			pexprCTEConsumer->AddRef();
			pdrgpexprPrjElems->AddRef();
			pexprNewGbAgg =
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrGrpCols, COperator::EgbaggtypeGlobal),
					pexprCTEConsumer,
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprPrjElems)
					);
		}
		else
		{
			// create a new consumer
			DrgPcr *pdrgpcrNewConsumerOutput = CUtils::PdrgpcrCopy(pmp, pdrgpcrConsumerOutput);
			CExpression *pexprNewConsumer =
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcrNewConsumerOutput));
			pcteinfo->IncrementConsumers(ulCTEId);

			// fix Aggs arguments to use new consumer output column
			HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, pdrgpcrConsumerOutput, pdrgpcrNewConsumerOutput);
			DrgPexpr *pdrgpexprNewPrjElems = GPOS_NEW(pmp) DrgPexpr(pmp);
			const ULONG ulPrjElems = pdrgpexprPrjElems->UlLength();
			for (ULONG ul = 0; ul < ulPrjElems; ul++)
			{
				CExpression *pexprPrjEl = (*pdrgpexprPrjElems)[ul];

				// to match requested columns upstream, we have to re-use the same computed
				// columns that define the aggregates, we avoid creating new columns during
				// expression copy by passing fMustExist as false
				CExpression *pexprNewPrjEl = pexprPrjEl->PexprCopyWithRemappedColumns(pmp, phmulcr, false /*fMustExist*/);
				pdrgpexprNewPrjElems->Append(pexprNewPrjEl);
			}

			// re-map grouping columns
			DrgPcr *pdrgpcrNewGrpCols = CUtils::PdrgpcrRemap(pmp, pdrgpcrGrpCols, phmulcr, true /*fMustExist*/);

			// create new GbAgg expression
			pexprNewGbAgg =
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrNewGrpCols, COperator::EgbaggtypeGlobal),
					pexprNewConsumer,
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprNewPrjElems)
					);

			phmulcr->Release();
		}

		ulConsumers++;

		DrgPcr *pdrgpcrNewGrpCols  = CLogicalGbAgg::PopConvert(pexprNewGbAgg->Pop())->Pdrgpcr();
		if (NULL != pexprLastGbAgg)
		{
			CExpression *pexprJoinCondition = NULL;
			if (0 == pdrgpcrLastGrpCols->UlLength())
			{
				GPOS_ASSERT(0 == pdrgpcrNewGrpCols->UlLength());

				pexprTrue->AddRef();
				pexprJoinCondition = pexprTrue;
			}
			else
			{
				GPOS_ASSERT(pdrgpcrLastGrpCols->UlLength() == pdrgpcrNewGrpCols->UlLength());

				pexprJoinCondition = CPredicateUtils::PexprINDFConjunction(pmp, pdrgpcrLastGrpCols, pdrgpcrNewGrpCols);
			}

			if (NULL == pexprJoin)
			{
				// create first join
				pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprLastGbAgg, pexprNewGbAgg, pexprJoinCondition);
			}
			else
			{
				// cascade joins
				pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprJoin, pexprNewGbAgg, pexprJoinCondition);
			}
		}

		pexprLastGbAgg = pexprNewGbAgg;
		pdrgpcrLastGrpCols = pdrgpcrNewGrpCols;
	}

	pdrgpdrgpexprPrjElems->Release();
	phmexprdrgpexpr->Release();
	pexprTrue->Release();

	return pexprJoin;
}

// EOF
