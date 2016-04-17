//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CExpressionPreprocessor.cpp
//
//	@doc:
//		Expression tree preprocessing routines, needed to prepare an input
//		logical expression to be optimized
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
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpos/common/CAutoTimer.h"
#include "gpopt/exception.h"

#include "gpopt/operators/CWindowPreprocessor.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CExpressionUtils.h"
#include "gpopt/operators/CExpressionFactorizer.h"
#include "gpopt/operators/CExpressionPreprocessor.h"
#include "gpopt/optimizer/COptimizerConfig.h"

#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/xforms/CXform.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"

#include "naucrates/traceflags/traceflags.h"

using namespace gpopt;

// maximum number of equality predicates to be derived from existing equalities
#define GPOPT_MAX_DERIVED_PREDS 50

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprEliminateSelfComparison
//
//	@doc:
//		Eliminate self comparisons in the given expression
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprEliminateSelfComparison
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	if (CUtils::FScalarCmp(pexpr))
	{
		return CPredicateUtils::PexprEliminateSelfComparison(pmp, pexpr);
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprEliminateSelfComparison(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprPruneSuperfluousEquality
//
//	@doc:
//		Remove superfluous equality operations
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprPruneSuperfluousEquality
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	if (pexpr->Pop()->FScalar())
	{
		return CPredicateUtils::PexprPruneSuperfluosEquality(pmp, pexpr);
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprPruneSuperfluousEquality(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprTrimExistentialSubqueries
//
//	@doc:
//		An existential subquery whose inner expression is a GbAgg
//		with no grouping columns is replaced with a Boolean constant
//
//		Example:
//
//			exists(select sum(i) from X) --> True
//			not exists(select sum(i) from X) --> False
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprTrimExistentialSubqueries
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (CUtils::FExistentialSubquery(pop))
	{
		CExpression *pexprInner = (*pexpr)[0];
		if (COperator::EopLogicalGbAgg == pexprInner->Pop()->Eopid() &&
				0 == CLogicalGbAgg::PopConvert(pexprInner->Pop())->Pdrgpcr()->UlLength())
		{
			GPOS_ASSERT(0 < (*pexprInner)[1]->UlArity() &&
					"Project list of GbAgg is expected to be non-empty");
			BOOL fValue = true;
			if (COperator::EopScalarSubqueryNotExists == pop->Eopid())
			{
				fValue = false;
			}
			return CUtils::PexprScalarConstBool(pmp, fValue);
		}
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprTrimExistentialSubqueries(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	if (CPredicateUtils::FAnd(pexpr))
	{
		return CPredicateUtils::PexprConjunction(pmp, pdrgpexprChildren);
	}

	if (CPredicateUtils::FOr(pexpr))
	{
		return CPredicateUtils::PexprDisjunction(pmp, pdrgpexprChildren);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprSimplifyQuantifiedSubqueries
//
//	@doc:
//		A quantified subquery with maxcard 1 is simplified as a scalar
//		subquery
//
//		Example:
//
//			a = ANY (select sum(i) from X) --> a = (select sum(i) from X)
//			a <> ALL (select sum(i) from X) --> a <> (select sum(i) from X)
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprSimplifyQuantifiedSubqueries
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (CUtils::FQuantifiedSubquery(pop) &&
		1 == CDrvdPropRelational::Pdprel((*pexpr)[0]->PdpDerive())->Maxcard().Ull())
	{
		CExpression *pexprInner = (*pexpr)[0];

		// skip intermediate unary nodes
		CExpression *pexprChild = pexprInner;
		COperator *popChild = pexprChild->Pop();
		while (NULL != pexprChild && CUtils::FLogicalUnary(popChild))
		{
			pexprChild = (*pexprChild)[0];
			popChild = pexprChild->Pop();
		}

		// inspect next node
		BOOL fGbAggWithoutGrpCols =
				COperator::EopLogicalGbAgg == popChild->Eopid() &&
				0 == CLogicalGbAgg::PopConvert(popChild)->Pdrgpcr()->UlLength();

		BOOL fOneRowConstTable = COperator::EopLogicalConstTableGet == popChild->Eopid() &&
				1 == CLogicalConstTableGet::PopConvert(popChild)->Pdrgpdrgpdatum()->UlLength();

		if (fGbAggWithoutGrpCols || fOneRowConstTable)
		{
			// quantified subquery with max card 1
			CExpression *pexprScalar = (*pexpr)[1];
			CScalarSubqueryQuantified *popSubqQuantified =
					CScalarSubqueryQuantified::PopConvert(pexpr->Pop());
			const CColRef *pcr = popSubqQuantified->Pcr();
			pexprInner->AddRef();
			CExpression *pexprSubquery =
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarSubquery(pmp, pcr, false /*fGeneratedByExist*/, true /*fGeneratedByQuantified*/), pexprInner);

			CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
			IMDId *pmdid = popSubqQuantified->PmdidOp();
			const CWStringConst *pstr = pmda->Pmdscop(pmdid)->Mdname().Pstr();
			pmdid->AddRef();
			pexprScalar->AddRef();

			return CUtils::PexprScalarCmp(pmp, pexprScalar, pexprSubquery, *pstr, pmdid);
		}
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprSimplifyQuantifiedSubqueries(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprUnnestScalarSubqueries
//
//	@doc:
//		Preliminary unnesting of scalar subqueries
//
//
//		Example:
//			Input:   SELECT k, (SELECT (SELECT Y.i FROM Y WHERE Y.j=X.j)) from X
//			Output:  SELECT k, (SELECT Y.i FROM Y WHERE Y.j=X.j) from X
//
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprUnnestScalarSubqueries
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	// look for a Project Element with a scalar subquery below it
	if (CUtils::FProjElemWithScalarSubq(pexpr))
	{
		// recursively process scalar subquery
		CExpression *pexprSubq = PexprUnnestScalarSubqueries(pmp, (*pexpr)[0]);

		// check if subquery is defined as a Project on Const Table
		CExpression *pexprSubqChild = (*pexprSubq)[0];
		if (CUtils::FProjectConstTableWithOneScalarSubq(pexprSubqChild))
		{
			CExpression *pexprConstTable = (*pexprSubqChild)[0];
			CExpression *pexprPrjList = (*pexprSubqChild)[1];
			GPOS_ASSERT(1 == pexprPrjList->UlArity());

			CExpression *pexprPrjElem = (*pexprPrjList)[0];
			CExpression *pexprInnerSubq = (*pexprPrjElem)[0];
			GPOS_ASSERT(COperator::EopScalarSubquery == pexprInnerSubq->Pop()->Eopid());

			// make sure that inner subquery has no outer references to Const Table
			// since Const Table will be eliminated in output expression
			CColRefSet *pcrsConstTableOutput = CDrvdPropRelational::Pdprel(pexprConstTable->PdpDerive())->PcrsOutput();
			CColRefSet *pcrsOuterRefs = CDrvdPropRelational::Pdprel((*pexprInnerSubq)[0]->PdpDerive())->PcrsOuter();
			if (0 == pcrsOuterRefs->CElements() || pcrsOuterRefs->FDisjoint(pcrsConstTableOutput))
			{
				// recursively process inner subquery
				CExpression *pexprUnnestedSubq = PexprUnnestScalarSubqueries(pmp, pexprInnerSubq);

				// the original subquery is processed and can be removed now
				pexprSubq->Release();

				// build the new Project Element after eliminating outer subquery
				pop->AddRef();
				return GPOS_NEW(pmp) CExpression(pmp, pop, pexprUnnestedSubq);
			}
		}

		// otherwise, return a Project Element with the processed outer subquery
		pop->AddRef();
		return GPOS_NEW(pmp) CExpression(pmp, pop, pexprSubq);
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprUnnestScalarSubqueries(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprRemoveSuperfluousLimit
//
//	@doc:
//		An intermediate limit is removed if it has neither row count
//		nor offset
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprRemoveSuperfluousLimit
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	// if current operator is a logical limit with zero offset, and no specified
	// row count, skip to limit's logical child
	if (COperator::EopLogicalLimit == pop->Eopid() &&
			CUtils::FHasZeroOffset(pexpr) &&
			!CLogicalLimit::PopConvert(pop)->FHasCount())
	{
		CLogicalLimit *popLgLimit = CLogicalLimit::PopConvert(pop);
		if (!popLgLimit->FTopLimitUnderDML() ||
				(popLgLimit->FTopLimitUnderDML() && GPOS_FTRACE(EopttraceRemoveOrderBelowDML)))
		{
			return PexprRemoveSuperfluousLimit(pmp, (*pexpr)[0]);
		}
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild =
			PexprRemoveSuperfluousLimit(pmp, (*pexpr)[ul]);

		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprRemoveSuperfluousOuterRefs
//
//	@doc:
//		Remove outer references from order spec inside limit, grouping columns
//		in GbAgg, and Partition/Order columns in window operators
//
//		Example, for the schema: t(a, b), s(i, j)
//		The query:
//			select * from t where a < all (select i from s order by j, b limit 1);
//		should be equivalent to:
//			select * from t where a < all (select i from s order by j limit 1);
//		after removing the outer reference (b) from the order by clause of the
//		subquery (all tuples in the subquery have the same value for the outer ref)
//
//		Similarly,
//			select * from t where a in (select count(i) from s group by j, b);
//		is equivalent to:
//			select * from t where a in (select count(i) from s group by j);
//
//		Similarly,
//			select * from t where a in (select row_number() over (partition by t.a order by t.b) from s);
//		is equivalent to:
//			select * from t where a in (select row_number() over () from s);
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprRemoveSuperfluousOuterRefs
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	COperator::EOperatorId eopid = pop->Eopid();
	BOOL fHasOuterRefs = (pop->FLogical() && CUtils::FHasOuterRefs(pexpr));

	pop->AddRef();
	if (fHasOuterRefs)
	{
		if (COperator::EopLogicalLimit == eopid)
		{
			CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOuter();

			CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pop);
			COrderSpec *pos = popLimit->Pos();
			COrderSpec *posNew = pos->PosExcludeColumns(pmp, pcrsOuter);

			pop->Release();
			pop = GPOS_NEW(pmp) CLogicalLimit
						(
						pmp,
						posNew,
						popLimit->FGlobal(),
						popLimit->FHasCount(),
						popLimit->FTopLimitUnderDML()
						);
		}
		else if (COperator::EopLogicalGbAgg == eopid)
		{
			CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOuter();

			CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pop);
			DrgPcr *pdrgpcr = CUtils::PdrgpcrExcludeColumns(pmp, popAgg->Pdrgpcr(), pcrsOuter);
			DrgPcr *pdrgpcrMinimal = popAgg->PdrgpcrMinimal();
			if (NULL != pdrgpcrMinimal)
			{
				pdrgpcrMinimal = CUtils::PdrgpcrExcludeColumns(pmp, pdrgpcrMinimal, pcrsOuter);
			}

			DrgPcr *pdrgpcrArgDQA = popAgg->PdrgpcrArgDQA();
			if (NULL != pdrgpcrArgDQA)
			{
				pdrgpcrArgDQA->AddRef();
			}

			pop->Release();
			pop = GPOS_NEW(pmp) CLogicalGbAgg
							(
							pmp,
							pdrgpcr,
							pdrgpcrMinimal,
							popAgg->Egbaggtype(),
							popAgg->FGeneratesDuplicates(),
							pdrgpcrArgDQA
							);
		}
		else if (COperator::EopLogicalSequenceProject == eopid)
		{
			(void) pexpr->PdpDerive();
			CExpressionHandle exprhdl(pmp);
			exprhdl.Attach(pexpr);
			exprhdl.DeriveProps(NULL /*pdpctxt*/);
			CLogicalSequenceProject *popSequenceProject = CLogicalSequenceProject::PopConvert(pop);
			if (popSequenceProject->FHasLocalOuterRefs(exprhdl))
			{
				COperator *popNew = popSequenceProject->PopRemoveLocalOuterRefs(pmp, exprhdl);
				pop->Release();
				pop = popNew;
			}
		}
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprRemoveSuperfluousOuterRefs(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprCollapseInnerJoins
//
//	@doc:
//		Collapse cascaded inner joins into NAry-joins
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprCollapseInnerJoins
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	const ULONG ulArity = pexpr->UlArity();

	if (CPredicateUtils::FInnerJoin(pexpr))
	{
		BOOL fCollapsed = false;
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		DrgPexpr *pdrgpexprPred = GPOS_NEW(pmp) DrgPexpr(pmp);
		for (ULONG ul = 0; ul < ulArity - 1; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];
			if (CPredicateUtils::FInnerJoin(pexprChild))
			{
				fCollapsed = true;
				CPredicateUtils::CollectChildren(pexprChild, pdrgpexpr, pdrgpexprPred);
			}
			else
			{
				// recursively process child expression
				CExpression *pexprNewChild = PexprCollapseInnerJoins(pmp, pexprChild);
				pdrgpexpr->Append(pexprNewChild);
			}
		}
		CExpression *pexprScalar = (*pexpr) [ulArity - 1];
		pexprScalar->AddRef();
		pdrgpexprPred->Append(pexprScalar);

		pdrgpexpr->Append(CPredicateUtils::PexprConjunction(pmp, pdrgpexprPred));

		CExpression *pexprNAryJoin = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalNAryJoin(pmp), pdrgpexpr);
		CExpression *pexprResult = pexprNAryJoin;
		if (fCollapsed)
		{
			// a join was collapsed with its children into NAry-Join, we need to recursively
			// process the created NAry join
			pexprResult = PexprCollapseInnerJoins(pmp, pexprNAryJoin);
			pexprNAryJoin->Release();
		}

		COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
		ULONG ulJoinArityLimit = poconf->Phint()->UlJoinArityForAssociativityCommutativity();

		// The last child of an n-ary join expression is the scalar expression
		if (pexprResult->UlArity() - 1 > ulJoinArityLimit)
		{
			GPOPT_DISABLE_XFORM(CXform::ExfJoinCommutativity);
			GPOPT_DISABLE_XFORM(CXform::ExfJoinAssociativity);
		}
		return pexprResult;
	}

	// current operator is not an inner-join, recursively process children
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprCollapseInnerJoins(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprCollapseProjects
//
//	@doc:
//		Collapse cascaded logical project operators
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprCollapseProjects
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulArity = pexpr->UlArity();
	// recursively process children
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprCollapseProjects(pmp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	CExpression *pexprNew = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
	CExpression *pexprCollapsed = CUtils::PexprCollapseProjects(pmp, pexprNew);

	if (NULL == pexprCollapsed)
	{
		return pexprNew;
	}

	pexprNew->Release();

	return pexprCollapsed;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprCollapseUnionUnionAll
//
//	@doc:
//		Collapse cascaded union/union all into an NAry union/union all operator
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprCollapseUnionUnionAll
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	const ULONG ulArity = pexpr->UlArity();

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	// recursively process children
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprCollapseUnionUnionAll(pmp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	CExpression *pexprNew = GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
	if (!CPredicateUtils::FUnionOrUnionAll(pexprNew))
	{
		return pexprNew;
	}

	// array of input children and its column references
	DrgPexpr *pdrgpexprNew = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgDrgPcr *pdrgdrgpcrOrig = CLogicalSetOp::PopConvert(pop)->PdrgpdrgpcrInput();
	DrgDrgPcr *pdrgdrgpcrNew = GPOS_NEW(pmp) DrgDrgPcr(pmp);

	BOOL fCollapsed = false;
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (CPredicateUtils::FCollapsibleChildUnionUnionAll(pexprNew, ul))
		{
			fCollapsed = true;
			CPredicateUtils::CollectGrandChildrenUnionUnionAll
							(
							pmp,
							pexprNew,
							ul,
							pdrgpexprNew,
							pdrgdrgpcrNew
							);
		}
		else
		{
			CExpression *pexprChild = (*pexprNew)[ul];
			pexprChild->AddRef();
			pdrgpexprNew->Append(pexprChild);

			DrgPcr *pdrgpcrInput = (*pdrgdrgpcrOrig)[ul];
			pdrgpcrInput->AddRef();
			pdrgdrgpcrNew->Append(pdrgpcrInput);
		}
	}

	if (!fCollapsed)
	{
		// clean up
		pdrgdrgpcrNew->Release();
		pdrgpexprNew->Release();

		return pexprNew;
	}

	COperator *popNew = NULL;
	DrgPcr *pdrgpcrOutput = CLogicalSetOp::PopConvert(pop)->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();
	if (pop->Eopid() == COperator::EopLogicalUnion)
	{
		popNew = GPOS_NEW(pmp) CLogicalUnion(pmp, pdrgpcrOutput, pdrgdrgpcrNew);
	}
	else
	{
		GPOS_ASSERT(pop->Eopid() == COperator::EopLogicalUnionAll);
		popNew = GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrOutput, pdrgdrgpcrNew);
	}

	// clean up
	pexprNew->Release();

	return GPOS_NEW(pmp) CExpression(pmp, popNew, pdrgpexprNew);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprOuterJoinToInnerJoin
//
//	@doc:
//		Transform outer joins into inner joins
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprOuterJoinToInnerJoin
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	const ULONG ulArity = pexpr->UlArity();

	if (COperator::EopLogicalSelect == pop->Eopid() &&
		COperator::EopLogicalLeftOuterJoin == (*pexpr)[0]->Pop()->Eopid())
	{
		// a Select on top of LOJ can be turned into InnerJoin by normalization
		return CNormalizer::PexprNormalize(pmp, pexpr);
	}

	if (CPredicateUtils::FInnerJoin(pexpr))
	{
		// the predicates of an inner join on top of outer join can be used to turn the child outer join into another inner join
		CExpression *pexprScalar = (*pexpr)[ulArity - 1];
		DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];
			BOOL fNewChild = false;
			if (COperator::EopLogicalLeftOuterJoin == pexprChild->Pop()->Eopid())
			{
				CColRefSet *pcrsLOJInnerOutput = CDrvdPropRelational::Pdprel((*pexprChild)[1]->PdpDerive())->PcrsOutput();
				if (!GPOS_FTRACE(EopttraceDisableOuterJoin2InnerJoinRewrite) &&
					CPredicateUtils::FNullRejecting(pmp, pexprScalar, pcrsLOJInnerOutput))
				{
					CExpression *pexprNewOuter = PexprOuterJoinToInnerJoin(pmp, (*pexprChild)[0]);
					CExpression *pexprNewInner = PexprOuterJoinToInnerJoin(pmp, (*pexprChild)[1]);
					CExpression *pexprNewScalar = PexprOuterJoinToInnerJoin(pmp, (*pexprChild)[2]);
					CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprNewOuter, pexprNewInner, pexprNewScalar);
					pexprChild = PexprCollapseInnerJoins(pmp, pexprJoin);
					pexprJoin->Release();
					fNewChild = true;
				}
			}

			if (!fNewChild)
			{
				pexprChild = PexprOuterJoinToInnerJoin(pmp, pexprChild);
			}
			pdrgpexprChildren->Append(pexprChild);
		}

		return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalNAryJoin(pmp), pdrgpexprChildren);
	}

	// current operator is not an NAry-join, recursively process children
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprOuterJoinToInnerJoin(pmp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprConjEqualityPredicates
//
//	@doc:
// 		Generate equality predicates between the columns in the given set,
//		we cap the number of generated predicates by GPOPT_MAX_DERIVED_PREDS
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprConjEqualityPredicates
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pcrs);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	ULONG ulPreds = 0;
	CColRefSetIter crsiRight(*pcrs);
	while
		(
		crsiRight.FAdvance() &&
		GPOPT_MAX_DERIVED_PREDS > ulPreds
		)
	{
		CColRef *pcrRight = crsiRight.Pcr();

		CColRefSetIter crsiLeft(*pcrs);
		while
			(
			crsiLeft.FAdvance() &&
			GPOPT_MAX_DERIVED_PREDS > ulPreds
			)
		{
			CColRef *pcrLeft = crsiLeft.Pcr();
			if (pcrLeft == pcrRight)
			{
				break;
			}

			pdrgpexpr->Append(CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight));
			ulPreds++;
		}
	}

	return CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::FEquivClassFromChild
//
//	@doc:
// 		Check if all columns in the given equivalent class come from one of the
//		children of the given expression
//
//---------------------------------------------------------------------------
BOOL
CExpressionPreprocessor::FEquivClassFromChild
	(
	CColRefSet *pcrs,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != pexpr);

	const ULONG ulChildren = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (!pexprChild->Pop()->FLogical())
		{
			continue;
		}
		CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive());
		DrgPcrs *pdrgpcrs = pdprel->Ppc()->PdrgpcrsEquivClasses();
		if (pcrs->FContained(pdrgpcrs))
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprAddEqualityPreds
//
//	@doc:
// 		Additional equality predicates are generated based on the equivalence
//		classes in the constraint properties of the expression.
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprAddEqualityPreds
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRefSet *pcrsProcessed
	)
{
	GPOS_ASSERT(NULL != pcrsProcessed);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());

	const ULONG ulChildren = pexpr->UlArity();
	CPropConstraint *ppc = pdprel->Ppc();

	CExpression *pexprPred = NULL;
	COperator *pop = pexpr->Pop();
	if (CUtils::FLogicalDML(pop))
	{
		pexprPred = CUtils::PexprScalarConstBool(pmp, true);
	}
	else
	{
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		DrgPcrs *pdrgpcrs = ppc->PdrgpcrsEquivClasses();
		GPOS_ASSERT(NULL != pdrgpcrs);
		const ULONG ulEquivClasses = pdrgpcrs->UlLength();
		for (ULONG ul = 0; ul < ulEquivClasses; ul++)
		{
			CColRefSet *pcrsEquivClass = (*pdrgpcrs)[ul];

			CColRefSet *pcrsEquality = GPOS_NEW(pmp) CColRefSet(pmp);
			pcrsEquality->Include(pcrsEquivClass);
			pcrsEquality->Exclude(pcrsProcessed);

			// if equivalence class comes from any of the children, then skip it
			if (FEquivClassFromChild(pcrsEquality, pexpr))
			{
				pcrsEquality->Release();
				continue;
			}

			CExpression *pexprEquality = PexprConjEqualityPredicates(pmp, pcrsEquality);
			pcrsProcessed->Include(pcrsEquality);
			pcrsEquality->Release();
			pdrgpexpr->Append(pexprEquality);
		}

		pexprPred = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);
	}

	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FLogical())
		{
			CExpression *pexprChildNew = PexprAddEqualityPreds(pmp, pexprChild, pcrsProcessed);
			pdrgpexprChildren->Append(pexprChildNew);
		}
		else
		{
			pexprChild->AddRef();
			pdrgpexprChildren->Append(pexprChild);
		}
	}

	pop->AddRef();

	return CUtils::PexprSafeSelect
						(
						pmp,
						GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren),
						pexprPred
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprScalarPredicates
//
//	@doc:
// 		Generate predicates for the given set of columns based on the given
//		constraint property. Columns for which predicates are generated will be
//		added to the set of processed columns
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprScalarPredicates
	(
	IMemoryPool *pmp,
	CPropConstraint *ppc,
	CColRefSet *pcrsNotNull,
	CColRefSet *pcrs,
	CColRefSet *pcrsProcessed
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	CColRefSetIter crsi(*pcrs);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();

		CExpression *pexprScalar = ppc->PexprScalarMappedFromEquivCols(pmp, pcr);
		if (NULL == pexprScalar)
		{
			continue;
		}

		pcrsProcessed->Include(pcr);

		// do not add a NOT NULL predicate if column is not nullable or if it
		// already has another predicate on it
		if (CUtils::FScalarNotNull(pexprScalar) && (pcrsNotNull->FMember(pcr)
				|| ppc->Pcnstr()->FConstraint(pcr)))
		{
			pexprScalar->Release();
			continue;
		}
		pdrgpexpr->Append(pexprScalar);
	}

	if (0 == pdrgpexpr->UlLength())
	{
		pdrgpexpr->Release();
		return NULL;
	}

	return CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprFromConstraintsScalar
//
//	@doc:
// 		Process scalar expressions for generating additional predicates based on
//		derived constraints. This function is needed because scalar expressions
//		can have relational children when there are subqueries
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprFromConstraintsScalar
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	if (!CUtils::FHasSubquery(pexpr))
	{
		pexpr->AddRef();
		return pexpr;
	}

	const ULONG ulChildren = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FScalar())
		{
			pexprChild = PexprFromConstraintsScalar(pmp, pexprChild);
		}
		else
		{
			GPOS_ASSERT(pexprChild->Pop()->FLogical());
			CColRefSet *pcrsProcessed = GPOS_NEW(pmp) CColRefSet(pmp);
			pexprChild = PexprFromConstraints(pmp, pexprChild, pcrsProcessed);
			pcrsProcessed->Release();
		}

		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprWithImpliedPredsOnLOJInnerChild
//
//	@doc:
// 		Imply new predicates on LOJ's inner child based on constraints derived
//		from LOJ's outer child and join predicate
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprWithImpliedPredsOnLOJInnerChild
	(
	IMemoryPool *pmp,
	CExpression *pexprLOJ,
	BOOL *pfAddedPredicates // output: set to True if new predicates are added to inner child
	)
{
	GPOS_ASSERT(NULL != pexprLOJ);
	GPOS_ASSERT(NULL != pfAddedPredicates);
	GPOS_ASSERT(COperator::EopLogicalLeftOuterJoin == pexprLOJ->Pop()->Eopid());

	CExpression *pexprOuter = (*pexprLOJ)[0];
	CExpression *pexprInner = (*pexprLOJ)[1];
	CExpression *pexprOuterJoinPred = (*pexprLOJ)[2];

	// merge children constraints with constraints derived from join's predicate
	CExpressionHandle exprhdl(pmp);
	exprhdl.Attach(pexprLOJ);
	CPropConstraint *ppc = CLogical::PpcDeriveConstraintFromPredicates(pmp, exprhdl);

	// use the computed constraint to derive a scalar predicate on the inner child
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsInnerNotNull = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsNotNull();

	// generate a scalar predicate from the computed constraint, restricted to LOJ inner child
	CColRefSet *pcrsProcessed = GPOS_NEW(pmp) CColRefSet(pmp);
	CExpression *pexprPred = PexprScalarPredicates(pmp, ppc, pcrsInnerNotNull, pcrsInnerOutput, pcrsProcessed);
	pcrsProcessed->Release();
	ppc->Release();

	pexprInner->AddRef();
	if (NULL != pexprPred && !CUtils::FScalarConstTrue(pexprPred))
	{
		// if a new predicate was added, set the output flag to True
		*pfAddedPredicates = true;
		pexprPred->AddRef();
		CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprInner, pexprPred);
		CExpression *pexprInnerNormalized = CNormalizer::PexprNormalize(pmp, pexprSelect);
		pexprSelect->Release();
		pexprInner = pexprInnerNormalized;
	}
	CRefCount::SafeRelease(pexprPred);

	// recursively process inner child
	CExpression *pexprNewInner = PexprOuterJoinInferPredsFromOuterChildToInnerChild(pmp, pexprInner, pfAddedPredicates);
	pexprInner->Release();

	// recursively process outer child
	CExpression *pexprNewOuter = PexprOuterJoinInferPredsFromOuterChildToInnerChild(pmp, pexprOuter, pfAddedPredicates);

	pexprOuterJoinPred->AddRef();
	COperator *pop = pexprLOJ->Pop();
	pop->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, pop, pexprNewOuter, pexprNewInner, pexprOuterJoinPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprOuterJoinInferPredsFromOuterChildToInnerChild
//
//	@doc:
// 		Infer predicate from outer child to inner child of the outer join,
//
//		for LOJ expressions with predicates on outer child, e.g.,
//
//			+-LOJ(x=y)
//   			|---Select(x=5)
// 		    	|   	+----X
// 	       		+----Y
//
//		this function implies an equivalent predicate (y=5) on the inner child of LOJ:
//
//			+-LOJ(x=y)
//				|---Select(x=5)
//				|		+----X
//				+---Select(y=5)
//						+----Y
//
//		the correctness of this rewrite can be proven as follows:
//			- By removing all tuples from Y that do not satisfy (y=5), the LOJ
//			results, where x=y, are retained. The reason is that any such join result
//			must satisfy (x=5 ^ x=y) which implies that (y=5).
//
//			- LOJ results that correspond to tuples from X not joining with any tuple
//			from Y are also retained. The reason is that such join results can only be
//			produced if for all tuples in Y, we have (y!=5). By selecting Y tuples where (y=5),
//			if we end up with no Y tuples, the LOJ results will be generated by joining X with empty Y.
//			This is the same as joining with all tuples from Y with (y!=5). If we end up with
//			any tuple in Y satisfying (y=5), no LOJ results corresponding to X tuples not joining
//			with Y can be produced.
//
//		to implement this rewrite in a general form, we need to imply general constraints on
//		LOJ's inner child from constraints that exist on LOJ's outer child. The generated predicate
//		from this inference can only be inserted below LOJ (on top of the inner child), and cannot be
//		inserted on top of LOJ, otherwise we may wrongly convert LOJ to inner-join.
//
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprOuterJoinInferPredsFromOuterChildToInnerChild
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	BOOL *pfAddedPredicates // output: set to True if new predicates are added to inner child
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pfAddedPredicates);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalLeftOuterJoin == pop->Eopid())
	{
		return PexprWithImpliedPredsOnLOJInnerChild(pmp, pexpr, pfAddedPredicates);
	}

	// not an outer join, process children recursively
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulChildren = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprOuterJoinInferPredsFromOuterChildToInnerChild(pmp, (*pexpr)[ul], pfAddedPredicates);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprFromConstraints
//
//	@doc:
// 		Additional predicates are generated based on the derived constraint
//		properties of the expression. No predicates are generated for the columns
//		in the already processed set. This set is expanded with more columns
//		that get processed along the way
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprFromConstraints
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRefSet *pcrsProcessed
	)
{
	GPOS_ASSERT(NULL != pcrsProcessed);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());

	const ULONG ulChildren = pexpr->UlArity();
	CPropConstraint *ppc = pdprel->Ppc();
	CColRefSet *pcrsNotNull = pdprel->PcrsNotNull();

	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FScalar())
		{
			pexprChild = PexprFromConstraintsScalar(pmp, pexprChild);
			pdrgpexprChildren->Append(pexprChild);
			continue;
		}

		// we already called derive at the beginning, so child properties are already derived
		CDrvdPropRelational *pdprelChild = CDrvdPropRelational::Pdprel(pexprChild->Pdp(CDrvdProp::EptRelational));

		CColRefSet *pcrsOutChild = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsOutChild->Include(pdprelChild->PcrsOutput());
		pcrsOutChild->Exclude(pcrsProcessed);

		// generate predicates for the output columns of child
		CExpression *pexprPred = PexprScalarPredicates(pmp, ppc, pcrsNotNull, pcrsOutChild, pcrsProcessed);
		pcrsOutChild->Release();

		// process child
		CExpression *pexprChildNew = PexprFromConstraints(pmp, pexprChild, pcrsProcessed);

		if (NULL != pexprPred)
		{
			pdrgpexprChildren->Append(CUtils::PexprSafeSelect(pmp, pexprChildNew, pexprPred));
		}
		else
		{
			pdrgpexprChildren->Append(pexprChildNew);
		}
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprPruneEmptySubtrees
//
//	@doc:
// 		Eliminate subtrees that have a zero output cardinality, replacing them
//		with a const table get with the same output schema and zero tuples
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprPruneEmptySubtrees
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (pop->FLogical() && !CUtils::FLogicalDML(pop))
	{
		CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexpr->PdpDerive());

		// if maxcard = 0: return a const table get with same output columns and zero tuples
		if (0 == pdprel->Maxcard())
		{
			// output columns
			DrgPcr *pdrgpcr = pdprel->PcrsOutput()->Pdrgpcr(pmp);

			// empty output data
			DrgPdrgPdatum *pdrgpdrgpdatum = GPOS_NEW(pmp) DrgPdrgPdatum(pmp);

			COperator *popCTG = GPOS_NEW(pmp) CLogicalConstTableGet(pmp, pdrgpcr, pdrgpdrgpdatum);
			return GPOS_NEW(pmp) CExpression(pmp, popCTG);
		}
	}

	// process children
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulChildren = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprPruneEmptySubtrees(pmp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprRemoveUnusedCTEs
//
//	@doc:
// 		Eliminate CTE Anchors for CTEs that have zero consumers
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprRemoveUnusedCTEs
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalCTEAnchor == pop->Eopid())
	{
		ULONG ulId = CLogicalCTEAnchor::PopConvert(pop)->UlId();
		if (!COptCtxt::PoctxtFromTLS()->Pcteinfo()->FUsed(ulId))
		{
			GPOS_ASSERT(1 == pexpr->UlArity());
			return PexprRemoveUnusedCTEs(pmp, (*pexpr)[0]);
		}
	}

	// process children
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulChildren = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprRemoveUnusedCTEs(pmp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::CollectCTEPredicates
//
//	@doc:
//		For all consumers of the same CTE, collect all selection predicates
//		on top of these consumers, if any, and store them in hash map
//
//---------------------------------------------------------------------------
void
CExpressionPreprocessor::CollectCTEPredicates
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CTEPredsMap *phm
	)
{
	GPOS_CHECK_STACK_SIZE;

	if (
			COperator::EopLogicalSelect == pexpr->Pop()->Eopid() &&
			COperator::EopLogicalCTEConsumer == (*pexpr)[0]->Pop()->Eopid() &&
			0 == CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOuter()->CElements() // no outer references in selection predicate
		)
	{
		CExpression *pexprScalar = (*pexpr)[1];
		if (!CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->FHasSubquery())
		{
			CExpression *pexprChild = (*pexpr)[0];
			CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pexprChild->Pop());
			ULONG ulCTEId = popConsumer->UlCTEId();
			CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(ulCTEId);
			GPOS_ASSERT(NULL != pexprProducer);

			CLogicalCTEProducer *popProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop());
			HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, popConsumer->Pdrgpcr(), popProducer->Pdrgpcr());
			CExpression *pexprRemappedScalar = pexprScalar->PexprCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
			phmulcr->Release();

			DrgPexpr *pdrgpexpr = phm->PtLookup(&ulCTEId);
			if (NULL == pdrgpexpr)
			{
				pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif // GPOS_DEBUG
					phm->FInsert(GPOS_NEW(pmp) ULONG(ulCTEId), pdrgpexpr);
				GPOS_ASSERT(fInserted);
			}
			pdrgpexpr->Append(pexprRemappedScalar);
		}
	}

	// process children recursively
	const ULONG ulChildren = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CollectCTEPredicates(pmp, (*pexpr)[ul], phm);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::AddPredsToCTEProducers
//
//	@doc:
//		Add CTE predicates collected from consumers to producer expressions
//
//---------------------------------------------------------------------------
void
CExpressionPreprocessor::AddPredsToCTEProducers
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	CTEPredsMap *phm = GPOS_NEW(pmp) CTEPredsMap(pmp);
	CollectCTEPredicates(pmp, pexpr, phm);

	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	CTEPredsMapIter mi(phm);
	while (mi.FAdvance())
	{
		ULONG ulCTEId = *(mi.Pk());
		CExpression *pexprProducer = pcteinfo->PexprCTEProducer(ulCTEId);
		GPOS_ASSERT(NULL != pexprProducer);

		ULONG ulConsumers = pcteinfo->UlConsumers(ulCTEId);
		DrgPexpr *pdrgpexpr = const_cast<DrgPexpr *>(mi.Pt());

		// skip the propagation of predicate contains volatile function e.g. random() (value change within a scan)
		if (CPredicateUtils::FContainsVolatileFunction(pdrgpexpr))
		{
			continue;
		}

		if (0 < ulConsumers &&
			pdrgpexpr->UlLength() == ulConsumers)
		{
			// add new predicate to CTE producer only if all consumers have selection predicates,
			// for example, in the following query
			// 'with v as (select * from A) select * from v where a > 5 union select * from v where b > 5'
			// we add the new predicate '(a > 5 or b > 5)' to CTE producer expression,
			// while for the following query
			// 'with v as (select * from A) select * from v where a > 5 union select * from v'
			// we do not add any new predicates to CTE producer expression

			pdrgpexpr->AddRef();
			CExpression *pexprPred = CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr);
			(*pexprProducer)[0]->AddRef();
			CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, (*pexprProducer)[0], pexprPred);
			COperator *pop = pexprProducer->Pop();
			pop->AddRef();
			CExpression *pexprNewProducer = GPOS_NEW(pmp) CExpression(pmp, pop, pexprSelect);

			pcteinfo->ReplaceCTEProducer(pexprNewProducer);
			pexprNewProducer->Release();
		}
	}

	phm->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprAddPredicatesFromConstraints
//
//	@doc:
//		Derive constraints on given expression, and add new predicates
//		by implication
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprAddPredicatesFromConstraints
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// generate additional predicates from constraint properties
	CColRefSet *pcrsProcessed = GPOS_NEW(pmp) CColRefSet(pmp);
	CExpression *pexprConstraints = PexprFromConstraints(pmp, pexpr, pcrsProcessed);
	GPOS_CHECK_ABORT;
	pcrsProcessed->Release();

	// generate equality predicates for columns in equivalence classes
	pcrsProcessed = GPOS_NEW(pmp) CColRefSet(pmp);
	CExpression *pexprAddEqualityPreds = PexprAddEqualityPreds(pmp, pexprConstraints, pcrsProcessed);
	CExpression *pexprEqualityNormalized = CNormalizer::PexprNormalize(pmp, pexprAddEqualityPreds);
	GPOS_CHECK_ABORT;
	pcrsProcessed->Release();
	pexprConstraints->Release();
	pexprAddEqualityPreds->Release();

	// remove generated duplicate predicates
	CExpression *pexprDeduped = CExpressionUtils::PexprDedupChildren(pmp, pexprEqualityNormalized);
	pexprEqualityNormalized->Release();

	return pexprDeduped;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprInferPredicates
//
//	@doc:
//		 Driver for inferring predicates from constraints
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprInferPredicates
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	// generate new predicates from constraint properties and normalize the result
	CExpression *pexprWithPreds = PexprAddPredicatesFromConstraints(pmp, pexpr);

	// infer predicates from outer child to inner child of outer join
	BOOL fNewPreds = false;
	CExpression *pexprInferredPreds = PexprOuterJoinInferPredsFromOuterChildToInnerChild(pmp, pexprWithPreds, &fNewPreds);
	pexprWithPreds->Release();
	pexprWithPreds = pexprInferredPreds;

	if (fNewPreds)
	{
		// if we succeeded in generating new predicates below outer join, we need to
		// re-derive constraints to generate any other potential predicates
		pexprWithPreds = PexprAddPredicatesFromConstraints(pmp, pexprInferredPreds);
		pexprInferredPreds->Release();
	}

	return pexprWithPreds;
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprPruneUnusedComputedCols
//
//	@doc:
//		 Workhorse for pruning unused computed columns
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprPruneUnusedComputedCols
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRefSet *pcrsReqd
	)
{
	GPOS_ASSERT(NULL != pexpr);

	if (NULL == pcrsReqd || GPOS_FTRACE(EopttraceDisablePruneUnusedComputedColumns))
	{
		pexpr->AddRef();
		return pexpr;
	}

	COperator *pop = pexpr->Pop();

	// leave subquery alone
	if (CUtils::FSubquery(pop))
	{
		pexpr->AddRef();
		return pexpr;
	}

	CColRefSet *pcrsReqdNew = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsReqdNew->Include(pcrsReqd);

	if (COperator::EopLogicalProject == pop->Eopid() || COperator::EopLogicalGbAgg == pop->Eopid())
	{
		CExpression *pexprProjList = (*pexpr)[1];
		CColRefSet *pcrsDefined = CDrvdPropScalar::Pdpscalar(pexprProjList->PdpDerive())->PcrsDefined();
		CColRefSet *pcrsSetReturningFunction = CDrvdPropScalar::Pdpscalar(pexprProjList->PdpDerive())->PcrsSetReturningFunction();

		pcrsReqdNew->Include(CLogical::PopConvert(pop)->PcrsLocalUsed());
		// columns containing set-returning functions are needed for correct query results
		pcrsReqdNew->Union(pcrsSetReturningFunction);

		CColRefSet *pcrsUnusedLocal = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsUnusedLocal->Include(pcrsDefined);
		pcrsUnusedLocal->Difference(pcrsReqdNew);

		if (0 < pcrsUnusedLocal->CElements()) // need to prune
		{
			// actual construction of new operators without unnecessary project elements
			CExpression *pexprResult = PexprPruneProjListProjectOrGbAgg(pmp, pexpr, pcrsUnusedLocal, pcrsDefined, pcrsReqdNew);
			pcrsUnusedLocal->Release();
			pcrsReqdNew->Release();
			return pexprResult;
		}
		pcrsUnusedLocal->Release();
	}

	if (pop->FLogical())
	{
		// for logical operators, collect the used columns
		// this includes columns used by the operator itself and its scalar children
		CExpressionHandle exprhdl(pmp);
		exprhdl.Attach(pexpr);
		CColRefSet *pcrsLogicalUsed = exprhdl.PcrsUsedColumns(pmp);
		pcrsReqdNew->Include(pcrsLogicalUsed);
		pcrsLogicalUsed->Release();
	}

	// process children
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulChildren = pexpr->UlArity();

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprPruneUnusedComputedCols(pmp, (*pexpr)[ul], pcrsReqdNew);
		pdrgpexpr->Append(pexprChild);
	}
	pcrsReqdNew->Release();

	pop->AddRef();

	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexpr);

}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprPruneProjListProjectOrGbAgg
//
//	@doc:
//		Construct new Project or GroupBy operator without unused computed
//		columns as project elements
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprPruneProjListProjectOrGbAgg
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRefSet *pcrsUnused,
	CColRefSet *pcrsDefined,
	const CColRefSet *pcrsReqd
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pcrsUnused);
	GPOS_ASSERT(NULL != pcrsDefined);
	GPOS_ASSERT(NULL != pcrsReqd);

	CExpression *pexprResult = NULL;
	COperator *pop = pexpr->Pop();
	CColRefSet *pcrsReqdNew = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsReqdNew->Include(pcrsReqd);

	GPOS_ASSERT(COperator::EopLogicalProject == pop->Eopid() || COperator::EopLogicalGbAgg == pop->Eopid());

	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjList = (*pexpr)[1];

	// recursively process the relational child
	CExpression *pexprRelationalNew = NULL;

	if (pcrsUnused->CElements() == pcrsDefined->CElements())
	{
		// the entire project list needs to be pruned
		if (COperator::EopLogicalProject == pop->Eopid())
		{
			pexprRelationalNew = PexprPruneUnusedComputedCols(pmp, pexprRelational, pcrsReqdNew);
			pexprResult = pexprRelationalNew;
		}
		else
		{
			GPOS_ASSERT(COperator::EopLogicalGbAgg == pop->Eopid());

			CExpression *pexprProjectListNew = NULL;
			DrgPcr *pdrgpcrGroupingCols = CLogicalGbAgg::PopConvert(pop)->Pdrgpcr();
			if (0 < pdrgpcrGroupingCols->UlLength())
			{
				// if grouping cols exist, we need to maintain the GbAgg with an empty project list
				pexprProjectListNew = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp));
				pcrsReqdNew->Include(pdrgpcrGroupingCols);
			}
			else
			{
				// TODO:  10/15/2015: if there is no grouping cols, we could remove the entire GbAgg and plug in a ConstTableGet instead
				pexprProjList->AddRef();
				pexprProjectListNew = pexprProjList;
				CExpressionHandle exprhdl(pmp);
				exprhdl.Attach(pexpr);
				CColRefSet *pcrsLogicalUsed = exprhdl.PcrsUsedColumns(pmp);
				pcrsReqdNew->Include(pcrsLogicalUsed);
				pcrsLogicalUsed->Release();
			}
			pop->AddRef();
			pexprRelationalNew = PexprPruneUnusedComputedCols(pmp, pexprRelational, pcrsReqdNew);
			pexprResult = GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelationalNew, pexprProjectListNew);
		}
	}
	else
	{
		// only remove part of the project elements
		DrgPexpr *pdrgpexprPrElRemain = GPOS_NEW(pmp) DrgPexpr(pmp);
		const ULONG ulPrjEls = pexprProjList->UlArity();
		CExpressionHandle exprhdl(pmp);

		for (ULONG ul = 0; ul < ulPrjEls; ul++)
		{
			CExpression *pexprPrEl = (*pexprProjList)[ul];
			CScalarProjectElement *popPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());
			if (!pcrsUnused->FMember(popPrEl->Pcr()))
			{
				pexprPrEl->AddRef();
				pdrgpexprPrElRemain->Append(pexprPrEl);
				pcrsReqdNew->Include(CDrvdPropScalar::Pdpscalar(pexprPrEl->PdpDerive())->PcrsUsed());
			}
		}

		GPOS_ASSERT(0 < pdrgpexprPrElRemain->UlLength());
		CExpression *pexprNewProjectList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pdrgpexprPrElRemain);
		pop->AddRef();
		pexprRelationalNew = PexprPruneUnusedComputedCols(pmp, pexprRelational, pcrsReqdNew);
		pexprResult = GPOS_NEW(pmp) CExpression(pmp, pop, pexprRelationalNew, pexprNewProjectList);
	}

	pcrsReqdNew->Release();
	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionPreprocessor::PexprPreprocess
//
//	@doc:
//		Main driver,
//		preprocessing of input logical expression
//
//---------------------------------------------------------------------------
CExpression *
CExpressionPreprocessor::PexprPreprocess
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRefSet *pcrsOutputAndOrderCols // query output cols and cols used in the order specs
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	CAutoTimer at("\n[OPT]: Expression Preprocessing Time", GPOS_FTRACE(EopttracePrintOptStats));

	// (1) remove unused CTE anchors
	CExpression *pexprNoUnusedCTEs = PexprRemoveUnusedCTEs(pmp, pexpr);
	GPOS_CHECK_ABORT;

	// (2) remove intermediate superfluous limit
	CExpression *pexprSimplified = PexprRemoveSuperfluousLimit(pmp, pexprNoUnusedCTEs);
	GPOS_CHECK_ABORT;
	pexprNoUnusedCTEs->Release();

	// (3) remove superfluous outer references from the order spec in limits, grouping columns in GbAgg, and
	// Partition/Order columns in window operators
	CExpression *pexprOuterRefsEleminated = PexprRemoveSuperfluousOuterRefs(pmp, pexprSimplified);
	GPOS_CHECK_ABORT;
	pexprSimplified->Release();

	// (4) trim unnecessary existential subqueries
	CExpression *pexprTrimmed = PexprTrimExistentialSubqueries(pmp, pexprOuterRefsEleminated);
	GPOS_CHECK_ABORT;
	pexprOuterRefsEleminated->Release();

	// (5) remove superfluous equality
	CExpression *pexprTrimmed2 = PexprPruneSuperfluousEquality(pmp, pexprTrimmed);
	GPOS_CHECK_ABORT;
	pexprTrimmed->Release();

	// (6) simplify quantified subqueries
	CExpression *pexprSubqSimplified = PexprSimplifyQuantifiedSubqueries(pmp, pexprTrimmed2);
	GPOS_CHECK_ABORT;
	pexprTrimmed2->Release();

	// (7) do preliminary unnesting of scalar subqueries
	CExpression *pexprSubqUnnested = PexprUnnestScalarSubqueries(pmp, pexprSubqSimplified);
	GPOS_CHECK_ABORT;
	pexprSubqSimplified->Release();

	// (8) unnest AND/OR/NOT predicates
	CExpression *pexprUnnested = CExpressionUtils::PexprUnnest(pmp, pexprSubqUnnested);
	GPOS_CHECK_ABORT;
	pexprSubqUnnested->Release();

	// (9) infer predicates from constraints
	CExpression *pexprInferredPreds = PexprInferPredicates(pmp, pexprUnnested);
	GPOS_CHECK_ABORT;
	pexprUnnested->Release();

	// (10) eliminate self comparisons
	CExpression *pexprSelfCompEliminated = PexprEliminateSelfComparison(pmp, pexprInferredPreds);
	GPOS_CHECK_ABORT;
	pexprInferredPreds->Release();

	// (11) remove duplicate AND/OR children
	CExpression *pexprDeduped = CExpressionUtils::PexprDedupChildren(pmp, pexprSelfCompEliminated);
	GPOS_CHECK_ABORT;
	pexprSelfCompEliminated->Release();

	// (12) factorize common expressions
	CExpression *pexprFactorized = CExpressionFactorizer::PexprFactorize(pmp, pexprDeduped);
	GPOS_CHECK_ABORT;
	pexprDeduped->Release();

	// (13) infer filters out of components of disjunctive filters
	CExpression *pexprPrefiltersExtracted =
			CExpressionFactorizer::PexprExtractInferredFilters(pmp, pexprFactorized);
	GPOS_CHECK_ABORT;
	pexprFactorized->Release();

	// (14) pre-process window functions
	CExpression *pexprWindowPreprocessed = CWindowPreprocessor::PexprPreprocess(pmp, pexprPrefiltersExtracted);
	GPOS_CHECK_ABORT;
	pexprPrefiltersExtracted->Release();

	// (15) collapse cascaded union / union all
	CExpression *pexprNaryUnionUnionAll = PexprCollapseUnionUnionAll(pmp, pexprWindowPreprocessed);
	pexprWindowPreprocessed->Release();

	// (16) eliminate unused computed columns
	CExpression *pexprNoUnusedPrEl = PexprPruneUnusedComputedCols(pmp, pexprNaryUnionUnionAll, pcrsOutputAndOrderCols);
	GPOS_CHECK_ABORT;
	pexprNaryUnionUnionAll->Release();

	// (17) normalize expression
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(pmp, pexprNoUnusedPrEl);
	GPOS_CHECK_ABORT;
	pexprNoUnusedPrEl->Release();

	// (18) transform outer join into inner join whenever possible
	CExpression *pexprLOJToIJ = PexprOuterJoinToInnerJoin(pmp, pexprNormalized);
	GPOS_CHECK_ABORT;
	pexprNormalized->Release();

	// (19) collapse cascaded inner joins
	CExpression *pexprCollapsed = PexprCollapseInnerJoins(pmp, pexprLOJToIJ);
	GPOS_CHECK_ABORT;
	pexprLOJToIJ->Release();

	// (20) after transforming outer joins to inner joins, we may be able to generate more predicates from constraints
	CExpression *pexprWithPreds = PexprAddPredicatesFromConstraints(pmp, pexprCollapsed);
	GPOS_CHECK_ABORT;
	pexprCollapsed->Release();

	// (21) eliminate empty subtrees
	CExpression *pexprPruned = PexprPruneEmptySubtrees(pmp, pexprWithPreds);
	GPOS_CHECK_ABORT;
	pexprWithPreds->Release();

	// (22) collapse cascade of projects
	CExpression *pexprCollapsedProjects = PexprCollapseProjects(pmp, pexprPruned);
	pexprPruned->Release();

	return pexprCollapsedProjects;
}

// EOF
