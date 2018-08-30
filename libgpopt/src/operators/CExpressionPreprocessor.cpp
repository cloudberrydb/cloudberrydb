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
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpos/common/CAutoTimer.h"
#include "gpos/common/CAutoRef.h"
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

// eliminate self comparisons in the given expression
CExpression *
CExpressionPreprocessor::PexprEliminateSelfComparison
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	if (CUtils::FScalarCmp(pexpr))
	{
		return CPredicateUtils::PexprEliminateSelfComparison(mp, pexpr);
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprEliminateSelfComparison(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// remove superfluous equality operations
CExpression *
CExpressionPreprocessor::PexprPruneSuperfluousEquality
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	if (pexpr->Pop()->FScalar())
	{
		return CPredicateUtils::PexprPruneSuperfluosEquality(mp, pexpr);
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprPruneSuperfluousEquality(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// an existential subquery whose inner expression is a GbAgg
// with no grouping columns is replaced with a Boolean constant
//
//		Example:
//
//			exists(select sum(i) from X) --> True
//			not exists(select sum(i) from X) --> False
CExpression *
CExpressionPreprocessor::PexprTrimExistentialSubqueries
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (CUtils::FExistentialSubquery(pop))
	{
		CExpression *pexprInner = (*pexpr)[0];
		if (COperator::EopLogicalGbAgg == pexprInner->Pop()->Eopid() &&
				0 == CLogicalGbAgg::PopConvert(pexprInner->Pop())->Pdrgpcr()->Size())
		{
			GPOS_ASSERT(0 < (*pexprInner)[1]->Arity() &&
					"Project list of GbAgg is expected to be non-empty");
			BOOL fValue = true;
			if (COperator::EopScalarSubqueryNotExists == pop->Eopid())
			{
				fValue = false;
			}
			return CUtils::PexprScalarConstBool(mp, fValue);
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprTrimExistentialSubqueries(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	if (CPredicateUtils::FAnd(pexpr))
	{
		return CPredicateUtils::PexprConjunction(mp, pdrgpexprChildren);
	}

	if (CPredicateUtils::FOr(pexpr))
	{
		return CPredicateUtils::PexprDisjunction(mp, pdrgpexprChildren);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}



// a quantified subquery with maxcard 1 is simplified as a scalar subquery
//
// Example:
//		a = ANY (select sum(i) from X) --> a = (select sum(i) from X)
//		a <> ALL (select sum(i) from X) --> a <> (select sum(i) from X)
CExpression *
CExpressionPreprocessor::PexprSimplifyQuantifiedSubqueries
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (CUtils::FQuantifiedSubquery(pop) &&
		1 == CDrvdPropRelational::GetRelationalProperties((*pexpr)[0]->PdpDerive())->Maxcard().Ull())
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
				0 == CLogicalGbAgg::PopConvert(popChild)->Pdrgpcr()->Size();

		BOOL fOneRowConstTable = COperator::EopLogicalConstTableGet == popChild->Eopid() &&
				1 == CLogicalConstTableGet::PopConvert(popChild)->Pdrgpdrgpdatum()->Size();

		if (fGbAggWithoutGrpCols || fOneRowConstTable)
		{
			// quantified subquery with max card 1
			CExpression *pexprScalar = (*pexpr)[1];
			CScalarSubqueryQuantified *popSubqQuantified =
					CScalarSubqueryQuantified::PopConvert(pexpr->Pop());
			const CColRef *colref = popSubqQuantified->Pcr();
			pexprInner->AddRef();
			CExpression *pexprSubquery =
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarSubquery(mp, colref, false /*fGeneratedByExist*/, true /*fGeneratedByQuantified*/), pexprInner);

			CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
			IMDId *mdid = popSubqQuantified->MdIdOp();
			const CWStringConst *str = md_accessor->RetrieveScOp(mdid)->Mdname().GetMDName();
			mdid->AddRef();
			pexprScalar->AddRef();

			return CUtils::PexprScalarCmp(mp, pexprScalar, pexprSubquery, *str, mdid);
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprSimplifyQuantifiedSubqueries(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// preliminary unnesting of scalar subqueries
// Example:
// 		Input:   SELECT k, (SELECT (SELECT Y.i FROM Y WHERE Y.j=X.j)) from X
//		Output:  SELECT k, (SELECT Y.i FROM Y WHERE Y.j=X.j) from X
CExpression *
CExpressionPreprocessor::PexprUnnestScalarSubqueries
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	// look for a Project Element with a scalar subquery below it
	if (CUtils::FProjElemWithScalarSubq(pexpr))
	{
		// recursively process scalar subquery
		CExpression *pexprSubq = PexprUnnestScalarSubqueries(mp, (*pexpr)[0]);

		// if the scalar subquery is replaced by the CScalarIdent in the previous
		// recursive call we simply return the CScalarIdent and stop preprocessing
		// at this stage.
		// +--CScalarProjectList
		//	  +--CScalarProjectElement "?column?" (2)
		//	     +--CScalarIdent "column1" (1)
		if (COperator::EopScalarIdent == pexprSubq->Pop()->Eopid())
		{
			pop->AddRef();
			return GPOS_NEW(mp) CExpression(mp, pop, pexprSubq);
		}

		// check if subquery is defined as a Project on Const Table
		CExpression *pexprSubqChild = (*pexprSubq)[0];
		if (CUtils::FProjectConstTableWithOneScalarSubq(pexprSubqChild))
		{
			CExpression *pexprConstTable = (*pexprSubqChild)[0];
			CExpression *pexprPrjList = (*pexprSubqChild)[1];
			GPOS_ASSERT(1 == pexprPrjList->Arity());

			CExpression *pexprPrjElem = (*pexprPrjList)[0];
			CExpression *pexprInnerSubq = (*pexprPrjElem)[0];
			GPOS_ASSERT(COperator::EopScalarSubquery == pexprInnerSubq->Pop()->Eopid());

			// make sure that inner subquery has no outer references to Const Table
			// since Const Table will be eliminated in output expression
			CColRefSet *pcrsConstTableOutput = CDrvdPropRelational::GetRelationalProperties(pexprConstTable->PdpDerive())->PcrsOutput();
			CColRefSet *outer_refs = CDrvdPropRelational::GetRelationalProperties((*pexprInnerSubq)[0]->PdpDerive())->PcrsOuter();
			if (0 == outer_refs->Size() || outer_refs->IsDisjoint(pcrsConstTableOutput))
			{
				// recursively process inner subquery
				CExpression *pexprUnnestedSubq = PexprUnnestScalarSubqueries(mp, pexprInnerSubq);

				// the original subquery is processed and can be removed now
				pexprSubq->Release();

				// build the new Project Element after eliminating outer subquery
				pop->AddRef();
				return GPOS_NEW(mp) CExpression(mp, pop, pexprUnnestedSubq);
			}
		}

		// otherwise, return a Project Element with the processed outer subquery
		pop->AddRef();
		return GPOS_NEW(mp) CExpression(mp, pop, pexprSubq);
	}

	else if (CUtils::FScalarSubqWithConstTblGet(pexpr))
	{
		const CColRef *pcrSubq = CScalarSubquery::PopConvert(pexpr->Pop())->Pcr();
		CColRefSet *pcrsConstTableOutput = CDrvdPropRelational::GetRelationalProperties((*pexpr)[0]->PdpDerive())->PcrsOutput();

		// if the subquery has outer ref, we do not make use of the output columns of constant table get.
		// In this scenairo, we replace the entire scalar subquery with a CScalarIdent with the outer reference.
		// Otherwise, the subquery remains unchanged.
		// Input:
		//   +--CScalarSubquery["b" (8)]
		//      +--CLogicalConstTableGet Columns: ["" (16)] Values: [(1)]
		// Output:
		//   +--CScalarIdent "b" (8)
		if (!pcrsConstTableOutput->FMember(pcrSubq))
		{
			CScalarSubquery *pScalarSubquery = CScalarSubquery::PopConvert(pexpr->Pop());
			return CUtils::PexprScalarIdent(mp, pScalarSubquery->Pcr());
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprUnnestScalarSubqueries(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// an intermediate limit is removed if it has neither row count nor offset
CExpression *
CExpressionPreprocessor::PexprRemoveSuperfluousLimit
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	// if current operator is a logical limit with zero offset, and no specified
	// row count, skip to limit's logical child
	if (COperator::EopLogicalLimit == pop->Eopid() &&
			CUtils::FHasZeroOffset(pexpr) &&
			!CLogicalLimit::PopConvert(pop)->FHasCount())
	{
		CLogicalLimit *popLgLimit = CLogicalLimit::PopConvert(pop);
		if (!popLgLimit->IsTopLimitUnderDMLorCTAS() ||
				(popLgLimit->IsTopLimitUnderDMLorCTAS() && GPOS_FTRACE(EopttraceRemoveOrderBelowDML)))
		{
			return PexprRemoveSuperfluousLimit(mp, (*pexpr)[0]);
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild =
			PexprRemoveSuperfluousLimit(mp, (*pexpr)[ul]);

		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

//	Remove outer references from order spec inside limit, grouping columns
//	in GbAgg, and Partition/Order columns in window operators
//
//	Example, for the schema: t(a, b), s(i, j)
//	The query:
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
CExpression *
CExpressionPreprocessor::PexprRemoveSuperfluousOuterRefs
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	COperator::EOperatorId op_id = pop->Eopid();
	BOOL fHasOuterRefs = (pop->FLogical() && CUtils::HasOuterRefs(pexpr));

	pop->AddRef();
	if (fHasOuterRefs)
	{
		if (COperator::EopLogicalLimit == op_id)
		{
			CColRefSet *outer_refs = CDrvdPropRelational::GetRelationalProperties(pexpr->PdpDerive())->PcrsOuter();

			CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pop);
			COrderSpec *pos = popLimit->Pos();
			COrderSpec *posNew = pos->PosExcludeColumns(mp, outer_refs);

			pop->Release();
			pop = GPOS_NEW(mp) CLogicalLimit
						(
						mp,
						posNew,
						popLimit->FGlobal(),
						popLimit->FHasCount(),
						popLimit->IsTopLimitUnderDMLorCTAS()
						);
		}
		else if (COperator::EopLogicalGbAgg == op_id)
		{
			CColRefSet *outer_refs = CDrvdPropRelational::GetRelationalProperties(pexpr->PdpDerive())->PcrsOuter();

			CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pop);
			CColRefArray *colref_array = CUtils::PdrgpcrExcludeColumns(mp, popAgg->Pdrgpcr(), outer_refs);

			CExpression *pExprProjList = (*pexpr)[1];

			// It's only valid to remove the outer reference if:
			// the projection list is NOT empty
			// or
			// the outer references are NOT the ONLY Group By column
			//
			// For example:
			// -- Cannot remove t.b from groupby, because this will produce invalid plan
			// select a from t where c in (select distinct t.b from s)
			//
			// -- remove t.b from groupby is ok, because there is at least one agg function: count()
			// select a from t where c in (select count(s.j) from s group by t.b)
			//
			// -- remove t.b from groupby is ok, because there is other groupby column s.j
			// select a from t where c in (select s.j from s group by t.b, s.j)
			//
			// -- remove t.b from groupby is ok, because outer reference is a
			// -- constant for each invocation of subquery
			// select a from t where c in (select count(s.j) from s group by s.i, t.b)
			//
			if (0 < pExprProjList->Arity() || 0 < colref_array->Size())
			{
				CColRefArray *pdrgpcrMinimal = popAgg->PdrgpcrMinimal();
				if (NULL != pdrgpcrMinimal)
				{
					pdrgpcrMinimal = CUtils::PdrgpcrExcludeColumns(mp, pdrgpcrMinimal, outer_refs);
				}

				CColRefArray *pdrgpcrArgDQA = popAgg->PdrgpcrArgDQA();
				if (NULL != pdrgpcrArgDQA)
				{
					pdrgpcrArgDQA->AddRef();
				}

				pop->Release();
				pop = GPOS_NEW(mp) CLogicalGbAgg
								(
								mp,
								colref_array,
								pdrgpcrMinimal,
								popAgg->Egbaggtype(),
								popAgg->FGeneratesDuplicates(),
								pdrgpcrArgDQA
								);
			}
			else
			{
				// clean up
				colref_array->Release();
			}
		}
		else if (COperator::EopLogicalSequenceProject == op_id)
		{
			(void) pexpr->PdpDerive();
			CExpressionHandle exprhdl(mp);
			exprhdl.Attach(pexpr);
			exprhdl.DeriveProps(NULL /*pdpctxt*/);
			CLogicalSequenceProject *popSequenceProject = CLogicalSequenceProject::PopConvert(pop);
			if (popSequenceProject->FHasLocalOuterRefs(exprhdl))
			{
				COperator *popNew = popSequenceProject->PopRemoveLocalOuterRefs(mp, exprhdl);
				pop->Release();
				pop = popNew;
			}
		}
	}

	// recursively process children
	const ULONG arity = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprRemoveSuperfluousOuterRefs(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// generate a ScalarBoolOp expression or simply return the only expression
// in the array if there is only one.
CExpression *
CExpressionPreprocessor::PexprScalarBoolOpConvert2In
	(
	IMemoryPool *mp,
	CScalarBoolOp::EBoolOperator eboolop,
	CExpressionArray *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());

	if (1 == pdrgpexpr->Size())
	{
	    // if there is one child, do not wrap it in a bool op
	    CExpression *pexpr = (* pdrgpexpr)[0];
	    pexpr->AddRef();
	    pdrgpexpr->Release();
	    return pexpr;
	}

	return GPOS_NEW(mp) CExpression
			(
			mp,
			GPOS_NEW(mp) CScalarBoolOp(mp, eboolop),
			pdrgpexpr
			);
}

// checks if the given expression is likely to be simplified by the constraints
// framework during array conversion. eboolop is the CScalarBoolOp type
// of the expression which contains the argument expression
BOOL
CExpressionPreprocessor::FConvert2InIsConvertable(CExpression *pexpr, CScalarBoolOp::EBoolOperator eboolopParent)
{
	bool fConvertableExpression = false;
	if (CPredicateUtils::FCompareIdentToConst(pexpr))
	{
		fConvertableExpression |=
				IMDType::EcmptEq == CScalarCmp::PopConvert(pexpr->Pop())->ParseCmpType() &&
					CScalarBoolOp::EboolopOr == eboolopParent;
		fConvertableExpression |=
				IMDType::EcmptNEq == CScalarCmp::PopConvert(pexpr->Pop())->ParseCmpType() &&
					CScalarBoolOp::EboolopAnd == eboolopParent;
	}
	else if (CPredicateUtils::FCompareIdentToConstArray(pexpr))
	{
		fConvertableExpression = true;
	}

	if (fConvertableExpression)
	{
		GPOS_ASSERT(0 < pexpr->Arity());
		CScalarIdent *pscid = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		if (!CUtils::FConstrainableType(pscid->MdidType()))
		{
			fConvertableExpression = false;
		}
	}

	return fConvertableExpression;
}

// converts series of AND or OR comparisons into array IN expressions. For
// example, x = 1 OR x = 2 will convert to x IN (1,2). This stage assumes
// the expression has been unnested using CExpressionUtils::PexprUnnest.
CExpression *
CExpressionPreprocessor::PexprConvert2In
	(
	IMemoryPool *mp,
	CExpression *pexpr // does not take ownership
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (CPredicateUtils::FOr(pexpr) || CPredicateUtils::FAnd(pexpr) )
	{
		// the bool op type of this node
		CScalarBoolOp::EBoolOperator eboolop = CScalarBoolOp::PopConvert(pop)->Eboolop();
		// derive constraints on all of the simple scalar children
		// and add them to a new AND or OR expression
		CExpressionArray *pdrgpexprCollapse = GPOS_NEW(mp) CExpressionArray(mp);
		CExpressionArray *pdrgpexprRemainder = GPOS_NEW(mp) CExpressionArray(mp);
		const ULONG arity = pexpr->Arity();
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];

			if (FConvert2InIsConvertable(pexprChild, eboolop))
			{
				pexprChild->AddRef();
				pdrgpexprCollapse->Append(pexprChild);
			}
			else
			{
				// recursively convert the remainder and add to the array
				pdrgpexprRemainder->Append(PexprConvert2In(mp, pexprChild));
			}
		}

		if (0 != pdrgpexprCollapse->Size())
		{
			// create the constraint, rederive the collapsed expression
			// add the new derived expr to remainder
			CColRefSetArray *colref_array = NULL;
			pop->AddRef();
			CAutoRef<CExpression> apexprPreCollapse(GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprCollapse));
			CAutoRef<CConstraint> apcnst(CConstraint::PcnstrFromScalarExpr(mp, apexprPreCollapse.Value(), &colref_array));

			GPOS_ASSERT(NULL != apcnst.Value());
			CExpression *pexprPostCollapse = apcnst->PexprScalar(mp);

			pexprPostCollapse->AddRef();
			pdrgpexprRemainder->Append(pexprPostCollapse);
			CRefCount::SafeRelease(colref_array);
		}
		else
		{
			pdrgpexprCollapse->Release();
		}

		GPOS_ASSERT(0 < pdrgpexprRemainder->Size());
		return PexprScalarBoolOpConvert2In(mp, eboolop, pdrgpexprRemainder);
	}

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	CExpressionArray *pdrgexprChildren = pexpr->PdrgPexpr();
	for (ULONG ul = 0; ul < pexpr->Arity(); ul++)
	{
		pdrgpexpr->Append(PexprConvert2In(mp, (*pdrgexprChildren)[ul]));
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);

}

// collapse cascaded inner joins into NAry-joins
CExpression *
CExpressionPreprocessor::PexprCollapseInnerJoins
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	const ULONG arity = pexpr->Arity();

	if (CPredicateUtils::FInnerJoin(pexpr))
	{
		BOOL fCollapsed = false;
		CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
		CExpressionArray *pdrgpexprPred = GPOS_NEW(mp) CExpressionArray(mp);
		for (ULONG ul = 0; ul < arity - 1; ul++)
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
				CExpression *pexprNewChild = PexprCollapseInnerJoins(mp, pexprChild);
				pdrgpexpr->Append(pexprNewChild);
			}
		}
		CExpression *pexprScalar = (*pexpr) [arity - 1];
		pexprScalar->AddRef();
		pdrgpexprPred->Append(pexprScalar);

		pdrgpexpr->Append(CPredicateUtils::PexprConjunction(mp, pdrgpexprPred));

		CExpression *pexprNAryJoin = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalNAryJoin(mp), pdrgpexpr);
		CExpression *pexprResult = pexprNAryJoin;
		if (fCollapsed)
		{
			// a join was collapsed with its children into NAry-Join, we need to recursively
			// process the created NAry join
			pexprResult = PexprCollapseInnerJoins(mp, pexprNAryJoin);
			pexprNAryJoin->Release();
		}

		COptimizerConfig *optimizer_config = COptCtxt::PoctxtFromTLS()->GetOptimizerConfig();
		ULONG ulJoinArityLimit = optimizer_config->GetHint()->UlJoinArityForAssociativityCommutativity();

		// The last child of an n-ary join expression is the scalar expression
		if (pexprResult->Arity() - 1 > ulJoinArityLimit)
		{
			GPOPT_DISABLE_XFORM(CXform::ExfJoinCommutativity);
			GPOPT_DISABLE_XFORM(CXform::ExfJoinAssociativity);
		}
		return pexprResult;
	}

	// current operator is not an inner-join, recursively process children
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprCollapseInnerJoins(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// collapse cascaded logical project operators
CExpression *
CExpressionPreprocessor::PexprCollapseProjects
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pexpr->Arity();
	// recursively process children
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprCollapseProjects(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	CExpression *pexprNew = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
	CExpression *pexprCollapsed = CUtils::PexprCollapseProjects(mp, pexprNew);

	if (NULL == pexprCollapsed)
	{
		return pexprNew;
	}

	pexprNew->Release();

	return pexprCollapsed;
}

// insert dummy project element below scalar subquery when the (a) the scalar
// subquery is below a project and (b) output column is an outer reference
CExpression *
CExpressionPreprocessor::PexprProjBelowSubquery
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	BOOL fUnderPrList
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	/*
	 * Consider the following subquery:
	 * SELECT (SELECT foo.b from bar) FROM foo
	 * If bar is empty we should return null.
	 *
	 * For this query during DXL->Expr translation, the project element
	 * (SELECT b FROM bar) is represented as scalar subquery that returns
	 * an output column. To ensure that this scalar subquery under the project
	 * operator is returned when bar (or an arbitrary tree instead of bar)
	 * we insert a dummy project element that points to FOO.b under the
	 * scalar subquery. This dummy project element prevents its incorrect
	 * transformation into a non-correlated plan.
	 *
	 * One of the reasons we add this dummy project is to force the subquery
	 * handler transformation to not produce a de-correlated plan
	 * for queries such as this.
	 *
	 * We want to limit the of such introduction dummy projects only when the
	 * following conditions are all satisfied:
	 * a)  The scalar subquery is in the project element scalar tree
	 * Another use case: SELECT (SELECT foo.b from bar) + 1 FROM foo
	 * b) The output of the scalar subquery is the column from the outer expression.
	 * Consider the query: SELECT (SELECT foo.b + 5 from bar) FROM foo. In such cases,
	 * since the foo.b + 5 is a new computed column inside the subquery with its own
	 * project element, we do not need to add anything.
	 */
	BOOL fUnderPrListChild = fUnderPrList;
	COperator *pop = pexpr->Pop();

	if (pop->FLogical())
	{
		if (COperator::EopLogicalProject == pop->Eopid())
		{
			CExpression *pexprRel = (*pexpr)[0];
			CExpression *pexprRelNew = PexprProjBelowSubquery(mp, pexprRel, false /* fUnderPrList */);

			CExpression *pexprPrList = (*pexpr)[1];
			CExpression *pexprPrListNew = PexprProjBelowSubquery(mp, pexprPrList, true /* fUnderPrList */);

			return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalProject(mp), pexprRelNew, pexprPrListNew);
		}

		fUnderPrListChild = false;
	}
	else if (COperator::EopScalarSubquery == pop->Eopid() && fUnderPrList)
	{
		CExpression *pexprRel = (*pexpr)[0];
		CExpression *pexprRelNew = PexprProjBelowSubquery(mp, pexprRel, false /* fUnderPrList */);

		const CColRefSet *prcsOutput = CDrvdPropRelational::GetRelationalProperties(pexprRelNew->PdpDerive())->PcrsOutput();
		const CColRef *pcrSubquery = CScalarSubquery::PopConvert(pop)->Pcr();
		if (NULL != prcsOutput && !prcsOutput->FMember(pcrSubquery))
		{
			CColumnFactory *col_factory = COptCtxt::PoctxtFromTLS()->Pcf();
			CColRef *pcrNewSubquery = col_factory->PcrCreate(pcrSubquery->RetrieveType(), pcrSubquery->TypeModifier());

			CExpression *pexprPrEl = CUtils::PexprScalarProjectElement(mp, pcrNewSubquery, CUtils::PexprScalarIdent(mp, pcrSubquery));
			CExpression *pexprProjList =  GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pexprPrEl);
			CExpression *pexprProj = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalProject(mp), pexprRelNew, pexprProjList);

			CScalarSubquery *popSubq = GPOS_NEW(mp) CScalarSubquery(mp, pcrNewSubquery, false /*fGeneratedByExist*/, false /*fGeneratedByQuantified*/);

			CExpression *pexprResult = GPOS_NEW(mp) CExpression(mp, popSubq, pexprProj);
			return pexprResult;
		}

		pop->AddRef();
		return GPOS_NEW(mp) CExpression(mp, pop, pexprRelNew);
	}

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG arity = pexpr->Arity();
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprProjBelowSubquery(mp, (*pexpr)[ul], fUnderPrListChild);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// collapse cascaded union/union all into an NAry union/union all operator
CExpression *
CExpressionPreprocessor::PexprCollapseUnionUnionAll
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	const ULONG arity = pexpr->Arity();

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	// recursively process children
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprCollapseUnionUnionAll(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	CExpression *pexprNew = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
	if (!CPredicateUtils::FUnionOrUnionAll(pexprNew))
	{
		return pexprNew;
	}

	// array of input children and its column references
	CExpressionArray *pdrgpexprNew = GPOS_NEW(mp) CExpressionArray(mp);
	CColRef2dArray *pdrgdrgpcrOrig = CLogicalSetOp::PopConvert(pop)->PdrgpdrgpcrInput();
	CColRef2dArray *pdrgdrgpcrNew = GPOS_NEW(mp) CColRef2dArray(mp);

	BOOL fCollapsed = false;
	for (ULONG ul = 0; ul < arity; ul++)
	{
		if (CPredicateUtils::FCollapsibleChildUnionUnionAll(pexprNew, ul))
		{
			fCollapsed = true;
			CPredicateUtils::CollectGrandChildrenUnionUnionAll
							(
							mp,
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

			CColRefArray *pdrgpcrInput = (*pdrgdrgpcrOrig)[ul];
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
	CColRefArray *pdrgpcrOutput = CLogicalSetOp::PopConvert(pop)->PdrgpcrOutput();
	pdrgpcrOutput->AddRef();
	if (pop->Eopid() == COperator::EopLogicalUnion)
	{
		popNew = GPOS_NEW(mp) CLogicalUnion(mp, pdrgpcrOutput, pdrgdrgpcrNew);
	}
	else
	{
		GPOS_ASSERT(pop->Eopid() == COperator::EopLogicalUnionAll);
		popNew = GPOS_NEW(mp) CLogicalUnionAll(mp, pdrgpcrOutput, pdrgdrgpcrNew);
	}

	// clean up
	pexprNew->Release();

	return GPOS_NEW(mp) CExpression(mp, popNew, pdrgpexprNew);
}

// transform outer joins into inner joins
CExpression *
CExpressionPreprocessor::PexprOuterJoinToInnerJoin
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	const ULONG arity = pexpr->Arity();

	if (COperator::EopLogicalSelect == pop->Eopid() &&
		COperator::EopLogicalLeftOuterJoin == (*pexpr)[0]->Pop()->Eopid())
	{
		// a Select on top of LOJ can be turned into InnerJoin by normalization
		return CNormalizer::PexprNormalize(mp, pexpr);
	}

	if (CPredicateUtils::FInnerJoin(pexpr))
	{
		// the predicates of an inner join on top of outer join can be used to turn the child outer join into another inner join
		CExpression *pexprScalar = (*pexpr)[arity - 1];
		CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
		for (ULONG ul = 0; ul < arity; ul++)
		{
			CExpression *pexprChild = (*pexpr)[ul];
			BOOL fNewChild = false;
			if (COperator::EopLogicalLeftOuterJoin == pexprChild->Pop()->Eopid())
			{
				CColRefSet *pcrsLOJInnerOutput = CDrvdPropRelational::GetRelationalProperties((*pexprChild)[1]->PdpDerive())->PcrsOutput();
				if (!GPOS_FTRACE(EopttraceDisableOuterJoin2InnerJoinRewrite) &&
					CPredicateUtils::FNullRejecting(mp, pexprScalar, pcrsLOJInnerOutput))
				{
					CExpression *pexprNewOuter = PexprOuterJoinToInnerJoin(mp, (*pexprChild)[0]);
					CExpression *pexprNewInner = PexprOuterJoinToInnerJoin(mp, (*pexprChild)[1]);
					CExpression *pexprNewScalar = PexprOuterJoinToInnerJoin(mp, (*pexprChild)[2]);
					CExpression *pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>(mp, pexprNewOuter, pexprNewInner, pexprNewScalar);
					pexprChild = PexprCollapseInnerJoins(mp, pexprJoin);
					pexprJoin->Release();
					fNewChild = true;
				}
			}

			if (!fNewChild)
			{
				pexprChild = PexprOuterJoinToInnerJoin(mp, pexprChild);
			}
			pdrgpexprChildren->Append(pexprChild);
		}

		return GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CLogicalNAryJoin(mp), pdrgpexprChildren);
	}

	// current operator is not an NAry-join, recursively process children
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprOuterJoinToInnerJoin(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// generate equality predicates between the columns in the given set,
CExpression *
CExpressionPreprocessor::PexprConjEqualityPredicates
	(
	IMemoryPool *mp,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pcrs);

	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	ULONG ulPreds = 0;
	CColRefSetIter crsiRight(*pcrs);
	while
		(
		crsiRight.Advance() &&
		GPOPT_MAX_DERIVED_PREDS > ulPreds
		)
	{
		CColRef *pcrRight = crsiRight.Pcr();

		CColRefSetIter crsiLeft(*pcrs);
		while
			(
			crsiLeft.Advance() &&
			GPOPT_MAX_DERIVED_PREDS > ulPreds
			)
		{
			CColRef *pcrLeft = crsiLeft.Pcr();
			if (pcrLeft == pcrRight)
			{
				break;
			}

			pdrgpexpr->Append(CUtils::PexprScalarEqCmp(mp, pcrLeft, pcrRight));
			ulPreds++;
		}
	}

	return CPredicateUtils::PexprConjunction(mp, pdrgpexpr);
}

// check if all columns in the given equivalent class come from one of the
// children of the given expression
BOOL
CExpressionPreprocessor::FEquivClassFromChild
	(
	CColRefSet *pcrs,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != pexpr);

	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (!pexprChild->Pop()->FLogical())
		{
			continue;
		}
		CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(pexprChild->PdpDerive());
		CColRefSetArray *pdrgpcrs = pdprel->Ppc()->PdrgpcrsEquivClasses();
		if (pcrs->FContained(pdrgpcrs))
		{
			return true;
		}
	}

	return false;
}

// additional equality predicates are generated based on the equivalence
// classes in the constraint properties of the expression.
CExpression *
CExpressionPreprocessor::PexprAddEqualityPreds
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	CColRefSet *pcrsProcessed
	)
{
	GPOS_ASSERT(NULL != pcrsProcessed);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(pexpr->PdpDerive());

	const ULONG ulChildren = pexpr->Arity();
	CPropConstraint *ppc = pdprel->Ppc();

	CExpression *pexprPred = NULL;
	COperator *pop = pexpr->Pop();
	if (CUtils::FLogicalDML(pop))
	{
		pexprPred = CUtils::PexprScalarConstBool(mp, true);
	}
	else
	{
		CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
		CColRefSetArray *pdrgpcrs = ppc->PdrgpcrsEquivClasses();
		GPOS_ASSERT(NULL != pdrgpcrs);
		const ULONG ulEquivClasses = pdrgpcrs->Size();
		for (ULONG ul = 0; ul < ulEquivClasses; ul++)
		{
			CColRefSet *pcrsEquivClass = (*pdrgpcrs)[ul];

			CColRefSet *pcrsEquality = GPOS_NEW(mp) CColRefSet(mp);
			pcrsEquality->Include(pcrsEquivClass);
			pcrsEquality->Exclude(pcrsProcessed);

			// if equivalence class comes from any of the children, then skip it
			if (FEquivClassFromChild(pcrsEquality, pexpr))
			{
				pcrsEquality->Release();
				continue;
			}

			CExpression *pexprEquality = PexprConjEqualityPredicates(mp, pcrsEquality);
			pcrsProcessed->Include(pcrsEquality);
			pcrsEquality->Release();
			pdrgpexpr->Append(pexprEquality);
		}

		pexprPred = CPredicateUtils::PexprConjunction(mp, pdrgpexpr);
	}

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FLogical())
		{
			CExpression *pexprChildNew = PexprAddEqualityPreds(mp, pexprChild, pcrsProcessed);
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
						mp,
						GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren),
						pexprPred
						);
}

// generate predicates for the given set of columns based on the given
// constraint property. Columns for which predicates are generated will be
// added to the set of processed columns
CExpression *
CExpressionPreprocessor::PexprScalarPredicates
	(
	IMemoryPool *mp,
	CPropConstraint *ppc,
	CColRefSet *pcrsNotNull,
	CColRefSet *pcrs,
	CColRefSet *pcrsProcessed
	)
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	CColRefSetIter crsi(*pcrs);
	while (crsi.Advance())
	{
		CColRef *colref = crsi.Pcr();

		CExpression *pexprScalar = ppc->PexprScalarMappedFromEquivCols(mp, colref);
		if (NULL == pexprScalar)
		{
			continue;
		}

		pcrsProcessed->Include(colref);

		// do not add a NOT NULL predicate if column is not nullable or if it
		// already has another predicate on it
		if (CUtils::FScalarNotNull(pexprScalar) && (pcrsNotNull->FMember(colref)
				|| ppc->Pcnstr()->FConstraint(colref)))
		{
			pexprScalar->Release();
			continue;
		}
		pdrgpexpr->Append(pexprScalar);
	}

	if (0 == pdrgpexpr->Size())
	{
		pdrgpexpr->Release();
		return NULL;
	}

	return CPredicateUtils::PexprConjunction(mp, pdrgpexpr);
}

// process scalar expressions for generating additional predicates based on
// derived constraints. This function is needed because scalar expressions
// can have relational children when there are subqueries
CExpression *
CExpressionPreprocessor::PexprFromConstraintsScalar
	(
	IMemoryPool *mp,
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

	const ULONG ulChildren = pexpr->Arity();
	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FScalar())
		{
			pexprChild = PexprFromConstraintsScalar(mp, pexprChild);
		}
		else
		{
			GPOS_ASSERT(pexprChild->Pop()->FLogical());
			CColRefSet *pcrsProcessed = GPOS_NEW(mp) CColRefSet(mp);
			pexprChild = PexprFromConstraints(mp, pexprChild, pcrsProcessed);
			pcrsProcessed->Release();
		}

		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// Imply new predicates on LOJ's inner child based on constraints derived
// from LOJ's outer child and join predicate
CExpression *
CExpressionPreprocessor::PexprWithImpliedPredsOnLOJInnerChild
	(
	IMemoryPool *mp,
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
	CExpressionHandle exprhdl(mp);
	exprhdl.Attach(pexprLOJ);
	CPropConstraint *ppc = CLogical::PpcDeriveConstraintFromPredicates(mp, exprhdl);

	// use the computed constraint to derive a scalar predicate on the inner child
	CColRefSet *pcrsInnerOutput = CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsInnerNotNull = CDrvdPropRelational::GetRelationalProperties(pexprInner->PdpDerive())->PcrsNotNull();

	// generate a scalar predicate from the computed constraint, restricted to LOJ inner child
	CColRefSet *pcrsProcessed = GPOS_NEW(mp) CColRefSet(mp);
	CExpression *pexprPred = PexprScalarPredicates(mp, ppc, pcrsInnerNotNull, pcrsInnerOutput, pcrsProcessed);
	pcrsProcessed->Release();
	ppc->Release();

	pexprInner->AddRef();
	if (NULL != pexprPred && !CUtils::FScalarConstTrue(pexprPred))
	{
		// if a new predicate was added, set the output flag to True
		*pfAddedPredicates = true;
		pexprPred->AddRef();
		CExpression *pexprSelect = CUtils::PexprLogicalSelect(mp, pexprInner, pexprPred);
		CExpression *pexprInnerNormalized = CNormalizer::PexprNormalize(mp, pexprSelect);
		pexprSelect->Release();
		pexprInner = pexprInnerNormalized;
	}
	CRefCount::SafeRelease(pexprPred);

	// recursively process inner child
	CExpression *pexprNewInner = PexprOuterJoinInferPredsFromOuterChildToInnerChild(mp, pexprInner, pfAddedPredicates);
	pexprInner->Release();

	// recursively process outer child
	CExpression *pexprNewOuter = PexprOuterJoinInferPredsFromOuterChildToInnerChild(mp, pexprOuter, pfAddedPredicates);

	pexprOuterJoinPred->AddRef();
	COperator *pop = pexprLOJ->Pop();
	pop->AddRef();

	return GPOS_NEW(mp) CExpression(mp, pop, pexprNewOuter, pexprNewInner, pexprOuterJoinPred);
}

// Infer predicate from outer child to inner child of the outer join,
//
//	for LOJ expressions with predicates on outer child, e.g.,
//
//		+-LOJ(x=y)
//  		|---Select(x=5)
// 	    	|   	+----X
// 	   		+----Y
//
//	this function implies an equivalent predicate (y=5) on the inner child of LOJ:
//
//		+-LOJ(x=y)
//			|---Select(x=5)
//			|		+----X
//			+---Select(y=5)
//					+----Y
//
//	the correctness of this rewrite can be proven as follows:
//		- By removing all tuples from Y that do not satisfy (y=5), the LOJ
//		results, where x=y, are retained. The reason is that any such join result
//		must satisfy (x=5 ^ x=y) which implies that (y=5).
//
//		- LOJ results that correspond to tuples from X not joining with any tuple
//		from Y are also retained. The reason is that such join results can only be
//		produced if for all tuples in Y, we have (y!=5). By selecting Y tuples where (y=5),
//		if we end up with no Y tuples, the LOJ results will be generated by joining X with empty Y.
//		This is the same as joining with all tuples from Y with (y!=5). If we end up with
//		any tuple in Y satisfying (y=5), no LOJ results corresponding to X tuples not joining
//		with Y can be produced.
//
//	to implement this rewrite in a general form, we need to imply general constraints on
//	LOJ's inner child from constraints that exist on LOJ's outer child. The generated predicate
//	from this inference can only be inserted below LOJ (on top of the inner child), and cannot be
//	inserted on top of LOJ, otherwise we may wrongly convert LOJ to inner-join.
CExpression *
CExpressionPreprocessor::PexprOuterJoinInferPredsFromOuterChildToInnerChild
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	BOOL *pfAddedPredicates // output: set to True if new predicates are added to inner child
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pfAddedPredicates);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalLeftOuterJoin == pop->Eopid())
	{
		return PexprWithImpliedPredsOnLOJInnerChild(mp, pexpr, pfAddedPredicates);
	}

	// not an outer join, process children recursively
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprOuterJoinInferPredsFromOuterChildToInnerChild(mp, (*pexpr)[ul], pfAddedPredicates);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// additional predicates are generated based on the derived constraint
// properties of the expression. No predicates are generated for the columns
// in the already processed set. This set is expanded with more columns
// that get processed along the way
CExpression *
CExpressionPreprocessor::PexprFromConstraints
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	CColRefSet *pcrsProcessed
	)
{
	GPOS_ASSERT(NULL != pcrsProcessed);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FLogical());

	CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(pexpr->PdpDerive());

	const ULONG ulChildren = pexpr->Arity();
	CPropConstraint *ppc = pdprel->Ppc();
	CColRefSet *pcrsNotNull = pdprel->PcrsNotNull();

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		if (pexprChild->Pop()->FScalar())
		{
			pexprChild = PexprFromConstraintsScalar(mp, pexprChild);
			pdrgpexprChildren->Append(pexprChild);
			continue;
		}

		// process child
		CExpression *pexprChildNew = PexprFromConstraints(mp, pexprChild, pcrsProcessed);

		// we already called derive at the beginning, so child properties are already derived
		CDrvdPropRelational *pdprelChild = CDrvdPropRelational::GetRelationalProperties(pexprChild->Pdp(DrvdPropArray::EptRelational));

		CColRefSet *pcrsOutChild = GPOS_NEW(mp) CColRefSet(mp);

		// output columns on which predicates must be inferred
		pcrsOutChild->Include(pdprelChild->PcrsOutput());

		// exclude column references on which predicates had been already inferred,
		// this avoids generating duplicate predicates on the parent node if a
		// predicate has already been placed on the child.
		pcrsOutChild->Exclude(pcrsProcessed);

		// generate predicates for the output columns of child
		CExpression *pexprPred = PexprScalarPredicates(mp, ppc, pcrsNotNull, pcrsOutChild, pcrsProcessed);
		pcrsOutChild->Release();

		if (NULL != pexprPred)
		{
			pdrgpexprChildren->Append(CUtils::PexprSafeSelect(mp, pexprChildNew, pexprPred));
		}
		else
		{
			pdrgpexprChildren->Append(pexprChildNew);
		}
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();

	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);
}

// eliminate subtrees that have a zero output cardinality, replacing them
// with a const table get with the same output schema and zero tuples
CExpression *
CExpressionPreprocessor::PexprPruneEmptySubtrees
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (pop->FLogical() && !CUtils::FLogicalDML(pop))
	{
		CDrvdPropRelational *pdprel = CDrvdPropRelational::GetRelationalProperties(pexpr->PdpDerive());

		// if maxcard = 0: return a const table get with same output columns and zero tuples
		if (0 == pdprel->Maxcard())
		{
			// output columns
			CColRefArray *colref_array = pdprel->PcrsOutput()->Pdrgpcr(mp);

			// empty output data
			IDatum2dArray *pdrgpdrgpdatum = GPOS_NEW(mp) IDatum2dArray(mp);

			COperator *popCTG = GPOS_NEW(mp) CLogicalConstTableGet(mp, colref_array, pdrgpdrgpdatum);
			return GPOS_NEW(mp) CExpression(mp, popCTG);
		}
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprPruneEmptySubtrees(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// eliminate CTE Anchors for CTEs that have zero consumers
CExpression *
CExpressionPreprocessor::PexprRemoveUnusedCTEs
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (COperator::EopLogicalCTEAnchor == pop->Eopid())
	{
		ULONG id = CLogicalCTEAnchor::PopConvert(pop)->Id();
		if (!COptCtxt::PoctxtFromTLS()->Pcteinfo()->FUsed(id))
		{
			GPOS_ASSERT(1 == pexpr->Arity());
			return PexprRemoveUnusedCTEs(mp, (*pexpr)[0]);
		}
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);

	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprRemoveUnusedCTEs(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// for all consumers of the same CTE, collect all selection predicates
// on top of these consumers, if any, and store them in hash map
void
CExpressionPreprocessor::CollectCTEPredicates
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	CTEPredsMap *phm
	)
{
	GPOS_CHECK_STACK_SIZE;

	if (
			COperator::EopLogicalSelect == pexpr->Pop()->Eopid() &&
			COperator::EopLogicalCTEConsumer == (*pexpr)[0]->Pop()->Eopid() &&
			0 == CDrvdPropRelational::GetRelationalProperties(pexpr->PdpDerive())->PcrsOuter()->Size() // no outer references in selection predicate
		)
	{
		CExpression *pexprScalar = (*pexpr)[1];
		if (!CDrvdPropScalar::GetDrvdScalarProps(pexprScalar->PdpDerive())->FHasSubquery())
		{
			CExpression *pexprChild = (*pexpr)[0];
			CLogicalCTEConsumer *popConsumer = CLogicalCTEConsumer::PopConvert(pexprChild->Pop());
			ULONG ulCTEId = popConsumer->UlCTEId();
			CExpression *pexprProducer = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PexprCTEProducer(ulCTEId);
			GPOS_ASSERT(NULL != pexprProducer);

			CLogicalCTEProducer *popProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop());
			UlongToColRefMap *colref_mapping = CUtils::PhmulcrMapping(mp, popConsumer->Pdrgpcr(), popProducer->Pdrgpcr());
			CExpression *pexprRemappedScalar = pexprScalar->PexprCopyWithRemappedColumns(mp, colref_mapping, true /*must_exist*/);
			colref_mapping->Release();

			CExpressionArray *pdrgpexpr = phm->Find(&ulCTEId);
			if (NULL == pdrgpexpr)
			{
				pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif // GPOS_DEBUG
					phm->Insert(GPOS_NEW(mp) ULONG(ulCTEId), pdrgpexpr);
				GPOS_ASSERT(fInserted);
			}
			pdrgpexpr->Append(pexprRemappedScalar);
		}
	}

	// process children recursively
	const ULONG ulChildren = pexpr->Arity();
	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CollectCTEPredicates(mp, (*pexpr)[ul], phm);
	}
}

// add CTE predicates collected from consumers to producer expressions
void
CExpressionPreprocessor::AddPredsToCTEProducers
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	CTEPredsMap *phm = GPOS_NEW(mp) CTEPredsMap(mp);
	CollectCTEPredicates(mp, pexpr, phm);

	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	CTEPredsMapIter mi(phm);
	while (mi.Advance())
	{
		ULONG ulCTEId = *(mi.Key());
		CExpression *pexprProducer = pcteinfo->PexprCTEProducer(ulCTEId);
		GPOS_ASSERT(NULL != pexprProducer);

		ULONG ulConsumers = pcteinfo->UlConsumers(ulCTEId);
		CExpressionArray *pdrgpexpr = const_cast<CExpressionArray *>(mi.Value());

		// skip the propagation of predicate contains volatile function e.g. random() (value change within a scan)
		if (CPredicateUtils::FContainsVolatileFunction(pdrgpexpr))
		{
			continue;
		}

		if (0 < ulConsumers &&
			pdrgpexpr->Size() == ulConsumers)
		{
			// add new predicate to CTE producer only if all consumers have selection predicates,
			// for example, in the following query
			// 'with v as (select * from A) select * from v where a > 5 union select * from v where b > 5'
			// we add the new predicate '(a > 5 or b > 5)' to CTE producer expression,
			// while for the following query
			// 'with v as (select * from A) select * from v where a > 5 union select * from v'
			// we do not add any new predicates to CTE producer expression

			pdrgpexpr->AddRef();
			CExpression *pexprPred = CPredicateUtils::PexprDisjunction(mp, pdrgpexpr);
			(*pexprProducer)[0]->AddRef();
			CExpression *pexprSelect = CUtils::PexprLogicalSelect(mp, (*pexprProducer)[0], pexprPred);
			COperator *pop = pexprProducer->Pop();
			pop->AddRef();
			CExpression *pexprNewProducer = GPOS_NEW(mp) CExpression(mp, pop, pexprSelect);

			pcteinfo->ReplaceCTEProducer(pexprNewProducer);
			pexprNewProducer->Release();
		}
	}

	phm->Release();
}

// derive constraints on given expression, and add new predicates by implication
CExpression *
CExpressionPreprocessor::PexprAddPredicatesFromConstraints
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// generate additional predicates from constraint properties
	CColRefSet *pcrsProcessed = GPOS_NEW(mp) CColRefSet(mp);
	CExpression *pexprConstraints = PexprFromConstraints(mp, pexpr, pcrsProcessed);
	GPOS_CHECK_ABORT;
	pcrsProcessed->Release();

	// generate equality predicates for columns in equivalence classes
	pcrsProcessed = GPOS_NEW(mp) CColRefSet(mp);
	CExpression *pexprAddEqualityPreds = PexprAddEqualityPreds(mp, pexprConstraints, pcrsProcessed);
	CExpression *pexprEqualityNormalized = CNormalizer::PexprNormalize(mp, pexprAddEqualityPreds);
	GPOS_CHECK_ABORT;
	pcrsProcessed->Release();
	pexprConstraints->Release();
	pexprAddEqualityPreds->Release();

	// remove generated duplicate predicates
	CExpression *pexprDeduped = CExpressionUtils::PexprDedupChildren(mp, pexprEqualityNormalized);
	pexprEqualityNormalized->Release();

	return pexprDeduped;
}

// driver for inferring predicates from constraints
CExpression *
CExpressionPreprocessor::PexprInferPredicates
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	// generate new predicates from constraint properties and normalize the result
	CExpression *pexprWithPreds = PexprAddPredicatesFromConstraints(mp, pexpr);

	// infer predicates from outer child to inner child of outer join
	BOOL fNewPreds = false;
	CExpression *pexprInferredPreds = PexprOuterJoinInferPredsFromOuterChildToInnerChild(mp, pexprWithPreds, &fNewPreds);
	pexprWithPreds->Release();
	pexprWithPreds = pexprInferredPreds;

	if (fNewPreds)
	{
		// if we succeeded in generating new predicates below outer join, we need to
		// re-derive constraints to generate any other potential predicates
		pexprWithPreds = PexprAddPredicatesFromConstraints(mp, pexprInferredPreds);
		pexprInferredPreds->Release();
	}

	return pexprWithPreds;
}

//	Workhorse for pruning unused computed columns
//
//	The required columns passed by the query is passed to this pre-processing
//	stage and the list of columns are copied to a new list. This driver function
//	calls the PexprPruneUnusedComputedColsRecursive function with the copied
//	required column set. The original required columns set is not modified by
//	this preprocessor.
//
// 	Extra copy of the required columns set is avoided in each recursive call by
//	creating a one-time copy and passing it by reference for all the recursive
//	calls.
//
//	The functional behavior of the PruneUnusedComputedCols changed slightly
//	because we do not delete the required column set at the end of every
//	call but pass it to the next and consecutive recursive calls. However,
//	it is safe to add required columns by each operator we traverse, because non
//	of the required columns from other child of a tree will appear on the project
//	list of the other children.
//
// Therefore, the added columns to the required columns which is caused by
// the recursive call and passing by reference will not have a bad affect
// on the overall result.
CExpression *
CExpressionPreprocessor::PexprPruneUnusedComputedCols
	(
	IMemoryPool *mp,
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
	CColRefSet *pcrsReqdNew = GPOS_NEW(mp) CColRefSet(mp);
	pcrsReqdNew->Include(pcrsReqd);

	CExpression *pExprNew = PexprPruneUnusedComputedColsRecursive(mp,pexpr,pcrsReqdNew);
	pcrsReqdNew->Release();
	return pExprNew;
}

// Workhorse for pruning unused computed columns
CExpression *
CExpressionPreprocessor::PexprPruneUnusedComputedColsRecursive
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	CColRefSet *pcrsReqd
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();

	// leave subquery alone
	if (CUtils::FSubquery(pop))
	{
		pexpr->AddRef();
		return pexpr;
	}

	if (COperator::EopLogicalProject == pop->Eopid() || COperator::EopLogicalGbAgg == pop->Eopid())
	{
		CExpression *pexprProjList = (*pexpr)[1];
		CColRefSet *pcrsDefined = CDrvdPropScalar::GetDrvdScalarProps(pexprProjList->PdpDerive())->PcrsDefined();
		CColRefSet *pcrsSetReturningFunction = CDrvdPropScalar::GetDrvdScalarProps(pexprProjList->PdpDerive())->PcrsSetReturningFunction();

		pcrsReqd->Include(CLogical::PopConvert(pop)->PcrsLocalUsed());
		// columns containing set-returning functions are needed for correct query results
		pcrsReqd->Union(pcrsSetReturningFunction);

		CColRefSet *pcrsUnusedLocal = GPOS_NEW(mp) CColRefSet(mp);
		pcrsUnusedLocal->Include(pcrsDefined);
		pcrsUnusedLocal->Difference(pcrsReqd);

		if (0 < pcrsUnusedLocal->Size()) // need to prune
		{
			// actual construction of new operators without unnecessary project elements
			CExpression *pexprResult = PexprPruneProjListProjectOrGbAgg(mp, pexpr, pcrsUnusedLocal, pcrsDefined, pcrsReqd);
			pcrsUnusedLocal->Release();
			return pexprResult;
		}
		pcrsUnusedLocal->Release();
	}

	if (pop->FLogical())
	{
		// for logical operators, collect the used columns
		// this includes columns used by the operator itself and its scalar children
		CExpressionHandle exprhdl(mp);
		exprhdl.Attach(pexpr);
		CColRefSet *pcrsLogicalUsed = exprhdl.PcrsUsedColumns(mp);
		pcrsReqd->Include(pcrsLogicalUsed);
		pcrsLogicalUsed->Release();
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulChildren = pexpr->Arity();

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprPruneUnusedComputedColsRecursive(mp, (*pexpr)[ul], pcrsReqd);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();

	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);

}

// Construct new Project or GroupBy operator without unused computed
// columns as project elements
CExpression *
CExpressionPreprocessor::PexprPruneProjListProjectOrGbAgg
	(
	IMemoryPool *mp,
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
	CColRefSet *pcrsReqdNew = GPOS_NEW(mp) CColRefSet(mp);
	pcrsReqdNew->Include(pcrsReqd);

	GPOS_ASSERT(COperator::EopLogicalProject == pop->Eopid() || COperator::EopLogicalGbAgg == pop->Eopid());

	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjList = (*pexpr)[1];

	// recursively process the relational child
	CExpression *pexprRelationalNew = NULL;

	if (pcrsUnused->Size() == pcrsDefined->Size())
	{
		// the entire project list needs to be pruned
		if (COperator::EopLogicalProject == pop->Eopid())
		{
			pexprRelationalNew = PexprPruneUnusedComputedColsRecursive(mp, pexprRelational, pcrsReqdNew);
			pexprResult = pexprRelationalNew;
		}
		else
		{
			GPOS_ASSERT(COperator::EopLogicalGbAgg == pop->Eopid());

			CExpression *pexprProjectListNew = NULL;
			CColRefArray *pdrgpcrGroupingCols = CLogicalGbAgg::PopConvert(pop)->Pdrgpcr();
			if (0 < pdrgpcrGroupingCols->Size())
			{
				// if grouping cols exist, we need to maintain the GbAgg with an empty project list
				pexprProjectListNew = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp));
				pcrsReqdNew->Include(pdrgpcrGroupingCols);
			}
			else
			{
				// TODO:  10/15/2015: if there is no grouping cols, we could remove the entire GbAgg and plug in a ConstTableGet instead
				pexprProjList->AddRef();
				pexprProjectListNew = pexprProjList;
				CExpressionHandle exprhdl(mp);
				exprhdl.Attach(pexpr);
				CColRefSet *pcrsLogicalUsed = exprhdl.PcrsUsedColumns(mp);
				pcrsReqdNew->Include(pcrsLogicalUsed);
				pcrsLogicalUsed->Release();
			}
			pop->AddRef();
			pexprRelationalNew = PexprPruneUnusedComputedColsRecursive(mp, pexprRelational, pcrsReqdNew);
			pexprResult = GPOS_NEW(mp) CExpression(mp, pop, pexprRelationalNew, pexprProjectListNew);
		}
	}
	else
	{
		// only remove part of the project elements
		CExpressionArray *pdrgpexprPrElRemain = GPOS_NEW(mp) CExpressionArray(mp);
		const ULONG ulPrjEls = pexprProjList->Arity();
		CExpressionHandle exprhdl(mp);

		for (ULONG ul = 0; ul < ulPrjEls; ul++)
		{
			CExpression *pexprPrEl = (*pexprProjList)[ul];
			CScalarProjectElement *popPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());
			if (!pcrsUnused->FMember(popPrEl->Pcr()))
			{
				pexprPrEl->AddRef();
				pdrgpexprPrElRemain->Append(pexprPrEl);
				pcrsReqdNew->Include(CDrvdPropScalar::GetDrvdScalarProps(pexprPrEl->PdpDerive())->PcrsUsed());
			}
		}

		GPOS_ASSERT(0 < pdrgpexprPrElRemain->Size());
		CExpression *pexprNewProjectList = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarProjectList(mp), pdrgpexprPrElRemain);
		pop->AddRef();
		pexprRelationalNew = PexprPruneUnusedComputedColsRecursive(mp, pexprRelational, pcrsReqdNew);
		pexprResult = GPOS_NEW(mp) CExpression(mp, pop, pexprRelationalNew, pexprNewProjectList);
	}

	pcrsReqdNew->Release();
	return pexprResult;
}

// reorder the child for scalar comparision to ensure that left child is a scalar ident and right child is a scalar const if not
CExpression *
CExpressionPreprocessor::PexprReorderScalarCmpChildren
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();
	if (CUtils::FScalarCmp(pexpr) || COperator::EopScalarIsDistinctFrom == pexpr->Pop()->Eopid())
	{
		GPOS_ASSERT(2 == pexpr->Arity());
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprRight = (*pexpr)[1];

		if (CUtils::FScalarConst(pexprLeft) && CUtils::FScalarIdent(pexprRight))
		{
			CScalarCmp *popScalarCmpCommuted = (dynamic_cast<CScalarCmp *>(pop))->PopCommutedOp(mp, pop);
			if (popScalarCmpCommuted)
			{
				pexprLeft->AddRef();
				pexprRight->AddRef();
				return GPOS_NEW(mp) CExpression(mp, popScalarCmpCommuted, pexprRight, pexprLeft);
			}
		}
	}

	// process children
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulChildren = pexpr->Arity();

	for (ULONG ul = 0; ul < ulChildren; ul++)
	{
		CExpression *pexprChild = PexprReorderScalarCmpChildren(mp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	pop->AddRef();
	return GPOS_NEW(mp) CExpression(mp, pop, pdrgpexpr);
}

// converts IN subquery with a project list to a predicate AND an EXISTS subquery
CExpression *
CExpressionPreprocessor::ConvertInToSimpleExists
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	COperator *pop = pexpr->Pop();
	CExpression *pexprLogicalProject = (*pexpr)[0];

	GPOS_ASSERT(COperator::EopLogicalProject == pexprLogicalProject->Pop()->Eopid());

	// generate scalarOp expression by using column referance of the IN subquery's inner
	// child's column referance as well as the expression extracted above from the
	// project element
	CExpression *pexprLeft = (*pexpr)[1];

	if (CUtils::FSubquery(pexprLeft->Pop()))
	{
		return NULL;
	}

	// since Orca doesn't support IN subqueries of multiple columns such as
	// (a,a) in (select foo.a, foo.a from ...) ,
	// we only extract the first expression under the first project element in the
	// project list and make it as the right operand to the scalar operation.
	CExpression *pexprRight = CUtils::PNthProjectElementExpr(pexprLogicalProject, 0);

	CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *mdid = CScalarSubqueryAny::PopConvert(pop)->MdIdOp();
	const CWStringConst *str = md_accessor->RetrieveScOp(mdid)->Mdname().GetMDName();

	mdid->AddRef();
	pexprRight->AddRef();
	pexprLeft->AddRef();

	CExpression *pexprScalarOp = CUtils::PexprScalarCmp(mp, pexprLeft, pexprRight, *str, mdid);

	// EXISTS subquery becomes the logical projects relational child.
	CExpression *pexprSubqOfExists = (*pexprLogicalProject)[0];
	pexprSubqOfExists->AddRef();
	CExpression *pexprScalarSubqExists = GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CScalarSubqueryExists(mp), pexprSubqOfExists);

	// AND the generated predicate with the EXISTS subquery expression and return.
	CExpressionArray *pdrgpexprBoolOperands = GPOS_NEW(mp) CExpressionArray(mp);

	pdrgpexprBoolOperands->Append(pexprScalarOp);
	pdrgpexprBoolOperands->Append(pexprScalarSubqExists);

	return CUtils::PexprScalarBoolOp(mp, CScalarBoolOp::EboolopAnd, pdrgpexprBoolOperands);
}

// rewrite IN subquery to EXIST subquery with a predicate
// Example:
// 		Input:   SELECT * FROM foo WHERE foo.a IN (SELECT foo.b+1 FROM bar);
//		Output:  SELECT * FROM foo WHERE foo.a=foo.b+1 AND EXISTS (SELECT * FROM bar);
CExpression *
CExpressionPreprocessor::PexprExistWithPredFromINSubq
	(
	IMemoryPool *mp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	COperator *pop = pexpr->Pop();

	// recursively process children
	const ULONG arity = pexpr->Arity();
	pop->AddRef();

	CExpressionArray *pdrgpexprChildren = GPOS_NEW(mp) CExpressionArray(mp);
	for (ULONG ul = 0; ul < arity; ul++)
	{
		CExpression *pexprChild = PexprExistWithPredFromINSubq(mp, (*pexpr)[ul]);
		pdrgpexprChildren->Append(pexprChild);
	}

	CExpression *pexprNew = GPOS_NEW(mp) CExpression(mp, pop, pdrgpexprChildren);

	//Check if the inner is a SubqueryAny
	if (CUtils::FAnySubquery(pop))
	{
		CExpression *pexprLogicalProject = (*pexprNew)[0];

		// we do the conversion if the project list has an outer reference and
		// it does not include any column from the relational child.
		if (COperator::EopLogicalProject != pexprLogicalProject->Pop()->Eopid() ||
			!CUtils::HasOuterRefs(pexprLogicalProject) ||
			CUtils::FInnerRefInProjectList(pexprLogicalProject))
		{
			return pexprNew;
		}

		CExpression *pexprNewConverted = ConvertInToSimpleExists(mp, pexprNew);
		if (NULL == pexprNewConverted)
		{
			return pexprNew;
		}

		pexprNew->Release();
		return pexprNewConverted;
	}

	return pexprNew;
}

// main driver, pre-processing of input logical expression
CExpression *
CExpressionPreprocessor::PexprPreprocess
	(
	IMemoryPool *mp,
	CExpression *pexpr,
	CColRefSet *pcrsOutputAndOrderCols // query output cols and cols used in the order specs
	)
{
	GPOS_ASSERT(NULL != mp);
	GPOS_ASSERT(NULL != pexpr);

	CAutoTimer at("\n[OPT]: Expression Preprocessing Time", GPOS_FTRACE(EopttracePrintOptimizationStatistics));

	// (1) remove unused CTE anchors
	CExpression *pexprNoUnusedCTEs = PexprRemoveUnusedCTEs(mp, pexpr);
	GPOS_CHECK_ABORT;

	// (2) remove intermediate superfluous limit
	CExpression *pexprSimplified = PexprRemoveSuperfluousLimit(mp, pexprNoUnusedCTEs);
	GPOS_CHECK_ABORT;
	pexprNoUnusedCTEs->Release();

	// (3) trim unnecessary existential subqueries
	CExpression * pexprTrimmed = PexprTrimExistentialSubqueries(mp, pexprSimplified);

	GPOS_CHECK_ABORT;
	pexprSimplified->Release();

	// (4) collapse cascaded union / union all
	CExpression *pexprNaryUnionUnionAll = PexprCollapseUnionUnionAll(mp, pexprTrimmed);

	GPOS_CHECK_ABORT;
	pexprTrimmed->Release();

	// (5) remove superfluous outer references from the order spec in limits, grouping columns in GbAgg, and
	// Partition/Order columns in window operators
	CExpression *pexprOuterRefsEleminated = PexprRemoveSuperfluousOuterRefs(mp, pexprNaryUnionUnionAll);

	GPOS_CHECK_ABORT;
	pexprNaryUnionUnionAll->Release();

	// (6) remove superfluous equality
	CExpression *pexprTrimmed2 = PexprPruneSuperfluousEquality(mp, pexprOuterRefsEleminated);
	GPOS_CHECK_ABORT;
	pexprOuterRefsEleminated->Release();

	// (7) simplify quantified subqueries
	CExpression *pexprSubqSimplified = PexprSimplifyQuantifiedSubqueries(mp, pexprTrimmed2);
	GPOS_CHECK_ABORT;
	pexprTrimmed2->Release();

	// (8) do preliminary unnesting of scalar subqueries
	CExpression *pexprSubqUnnested = PexprUnnestScalarSubqueries(mp, pexprSubqSimplified);
	GPOS_CHECK_ABORT;
	pexprSubqSimplified->Release();

	// (9) unnest AND/OR/NOT predicates
	CExpression *pexprUnnested = CExpressionUtils::PexprUnnest(mp, pexprSubqUnnested);
	GPOS_CHECK_ABORT;
	pexprSubqUnnested->Release();

	CExpression *pexprConvert2In = pexprUnnested;

	if (GPOS_FTRACE(EopttraceArrayConstraints))
	{
		// (9.5) ensure predicates are array IN or NOT IN where applicable
		pexprConvert2In = PexprConvert2In(mp, pexprUnnested);
		GPOS_CHECK_ABORT;
		pexprUnnested->Release();
	}

	// (10) infer predicates from constraints
	CExpression *pexprInferredPreds = PexprInferPredicates(mp, pexprConvert2In);
	GPOS_CHECK_ABORT;
	pexprConvert2In->Release();

	// (11) eliminate self comparisons
	CExpression *pexprSelfCompEliminated = PexprEliminateSelfComparison(mp, pexprInferredPreds);
	GPOS_CHECK_ABORT;
	pexprInferredPreds->Release();

	// (12) remove duplicate AND/OR children
	CExpression *pexprDeduped = CExpressionUtils::PexprDedupChildren(mp, pexprSelfCompEliminated);
	GPOS_CHECK_ABORT;
	pexprSelfCompEliminated->Release();

	// (13) factorize common expressions
	CExpression *pexprFactorized = CExpressionFactorizer::PexprFactorize(mp, pexprDeduped);
	GPOS_CHECK_ABORT;
	pexprDeduped->Release();

	// (14) infer filters out of components of disjunctive filters
	CExpression *pexprPrefiltersExtracted =
			CExpressionFactorizer::PexprExtractInferredFilters(mp, pexprFactorized);
	GPOS_CHECK_ABORT;
	pexprFactorized->Release();

	// (15) pre-process window functions
	CExpression *pexprWindowPreprocessed = CWindowPreprocessor::PexprPreprocess(mp, pexprPrefiltersExtracted);
	GPOS_CHECK_ABORT;
	pexprPrefiltersExtracted->Release();

	// (16) eliminate unused computed columns
	CExpression *pexprNoUnusedPrEl = PexprPruneUnusedComputedCols(mp, pexprWindowPreprocessed, pcrsOutputAndOrderCols);
	GPOS_CHECK_ABORT;
	pexprWindowPreprocessed->Release();

	// (17) normalize expression
	CExpression *pexprNormalized = CNormalizer::PexprNormalize(mp, pexprNoUnusedPrEl);
	GPOS_CHECK_ABORT;
	pexprNoUnusedPrEl->Release();

	// (18) transform outer join into inner join whenever possible
	CExpression *pexprLOJToIJ = PexprOuterJoinToInnerJoin(mp, pexprNormalized);
	GPOS_CHECK_ABORT;
	pexprNormalized->Release();

	// (19) collapse cascaded inner joins
	CExpression *pexprCollapsed = PexprCollapseInnerJoins(mp, pexprLOJToIJ);
	GPOS_CHECK_ABORT;
	pexprLOJToIJ->Release();

	// (20) after transforming outer joins to inner joins, we may be able to generate more predicates from constraints
	CExpression *pexprWithPreds = PexprAddPredicatesFromConstraints(mp, pexprCollapsed);
	GPOS_CHECK_ABORT;
	pexprCollapsed->Release();

	// (21) eliminate empty subtrees
	CExpression *pexprPruned = PexprPruneEmptySubtrees(mp, pexprWithPreds);
	GPOS_CHECK_ABORT;
	pexprWithPreds->Release();

	// (22) collapse cascade of projects
	CExpression *pexprCollapsedProjects = PexprCollapseProjects(mp, pexprPruned);
	GPOS_CHECK_ABORT;
	pexprPruned->Release();

	// (23) insert dummy project when the scalar subquery is under a project and returns an outer reference
	CExpression *pexprSubquery = PexprProjBelowSubquery(mp, pexprCollapsedProjects, false /* fUnderPrList */);
	GPOS_CHECK_ABORT;
	pexprCollapsedProjects->Release();

	// (24) reorder the children of scalar cmp operator to ensure that left child is scalar ident and right child is scalar const
	CExpression *pexrReorderedScalarCmpChildren = PexprReorderScalarCmpChildren(mp, pexprSubquery);
	GPOS_CHECK_ABORT;
	pexprSubquery->Release();

	// (25) rewrite IN subquery to EXIST subquery with a predicate
	CExpression *pexprExistWithPredFromINSubq = PexprExistWithPredFromINSubq(mp, pexrReorderedScalarCmpChildren);
	GPOS_CHECK_ABORT;
	pexrReorderedScalarCmpChildren->Release();

	return pexprExistWithPredFromINSubq;
}

// EOF
