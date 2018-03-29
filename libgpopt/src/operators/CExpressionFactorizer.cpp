//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CExpressionFactorizer.cpp
//
//	@doc:
//		, 
//
//	@owner:
//		Utility functions for expression factorization
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/exception.h"

#include "gpopt/operators/ops.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CExpressionFactorizer.h"
#include "gpopt/operators/CExpressionUtils.h"

#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDFunction.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprProcessDisjDescendents
//
//	@doc:
//		Visitor-like function to process descendents that are OR operators.
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprProcessDisjDescendents
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression *pexprLowestLogicalAncestor,
	PexprProcessDisj pfnpepdFunction
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	CExpression *pexprLogicalAncestor = pexprLowestLogicalAncestor;
	if (pexpr->Pop()->FLogical())
	{
		pexprLogicalAncestor = pexpr;
	}

	if (CPredicateUtils::FOr(pexpr))
	{
		return (*pfnpepdFunction)(pmp, pexpr, pexprLogicalAncestor);
	}

	// recursively process children
	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = PexprProcessDisjDescendents
					(
					pmp,
					(*pexpr)[ul],
					pexprLogicalAncestor,
					pfnpepdFunction
					);
		pdrgpexprChildren->Append(pexprChild);
	}

	COperator *pop = pexpr->Pop();
	pop->AddRef();
	return GPOS_NEW(pmp) CExpression(pmp, pop, pdrgpexprChildren);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::AddFactor
//
//	@doc:
//		Helper for adding a given expression either to the given factors
//		array or to a residuals array
//
//
//---------------------------------------------------------------------------
void CExpressionFactorizer::AddFactor
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPexpr *pdrgpexprFactors,
	DrgPexpr *pdrgpexprResidual,
	ExprMap *pexprmapFactors,
	ULONG
#ifdef GPOS_DEBUG
		ulDisjuncts
#endif // GPOS_DEBUG
	)
{
	ULONG *pul = pexprmapFactors->PtLookup(pexpr);
	GPOS_ASSERT_IMP(NULL != pul, ulDisjuncts == *pul);

	if (NULL != pul)
	{
		// check if factor already exist in factors array
		BOOL fFound = false;
		const ULONG ulSize = pdrgpexprFactors->UlLength();
		for (ULONG ul = 0; !fFound && ul < ulSize; ul++)
		{
			fFound = CUtils::FEqual(pexpr, (*pdrgpexprFactors)[ul]);
		}

		if (!fFound)
		{
			pexpr->AddRef();
			pdrgpexprFactors->Append(pexpr);
		}

		// replace factor with constant True in the residuals array
		pdrgpexprResidual->Append(CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/));
	}
	else
	{
		pexpr->AddRef();
		pdrgpexprResidual->Append(pexpr);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprmapFactors
//
//	@doc:
//		Helper for building a factors map
//
//		Example:
//		input:  (A=B AND B=C AND B>0) OR (A=B AND B=C AND A>0)
//		output: [(A=B, 2), (B=C, 2)]
//
//---------------------------------------------------------------------------
CExpressionFactorizer::ExprMap *
CExpressionFactorizer::PexprmapFactors
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr) && "input must be an OR expression");

	// a global (expression -> count) map
	ExprMap *pexprmapGlobal = GPOS_NEW(pmp) ExprMap(pmp);

	// factors map
	ExprMap *pexprmapFactors = GPOS_NEW(pmp) ExprMap(pmp);

	// iterate over child disjuncts;
	// if a disjunct is an AND tree, iterate over its children
	const ULONG ulDisjuncts = pexpr->UlArity();
	for (ULONG ulOuter = 0; ulOuter < ulDisjuncts; ulOuter++)
	{
		CExpression *pexprDisj = (*pexpr)[ulOuter];

		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		pexprDisj->AddRef();
		pdrgpexpr->Append(pexprDisj);

		if (CPredicateUtils::FAnd(pexprDisj))
		{
			pdrgpexpr->Release();
			pexprDisj->PdrgPexpr()->AddRef();
			pdrgpexpr = pexprDisj->PdrgPexpr();
		}

		const ULONG ulSize = pdrgpexpr->UlLength();
		for (ULONG ulInner = 0; ulInner < ulSize; ulInner++)
		{
			CExpression *pexprConj = (*pdrgpexpr)[ulInner];
			ULONG *pul = pexprmapGlobal->PtLookup(pexprConj);
			if (NULL == pul)
			{
				pexprConj->AddRef();
				(void) pexprmapGlobal->FInsert(pexprConj, GPOS_NEW(pmp) ULONG(1));
			}
			else
			{
				(*pul)++;
			}

			pul = pexprmapGlobal->PtLookup(pexprConj);
			if (*pul == ulDisjuncts)
			{
				// reached the count of initial disjuncts, add expression to factors map
				pexprConj->AddRef();
				(void) pexprmapFactors->FInsert(pexprConj, GPOS_NEW(pmp) ULONG(ulDisjuncts));
			}
		}
		pdrgpexpr->Release();
	}
	pexprmapGlobal->Release();

	return pexprmapFactors;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprFactorizeDisj
//
//	@doc:
//		Factorize common expressions in an OR tree;
//		the result is a conjunction of factors and a residual Or tree
//
//		Example:
//		input:  [(A=B AND C>0) OR (A=B AND A>0) OR (A=B AND B>0)]
//		output: [(A=B) AND (C>0 OR A>0 OR B>0)]
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprFactorizeDisj
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression * //pexprLogical
	)
{
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr) && "input must be an OR expression");

	// build factors map
	ExprMap *pexprmapFactors = PexprmapFactors(pmp, pexpr);

	DrgPexpr *pdrgpexprResidual = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprFactors = GPOS_NEW(pmp) DrgPexpr(pmp);

	// iterate over child expressions and factorize them
	const ULONG ulDisjuncts = pexpr->UlArity();
	for (ULONG ulOuter = 0; ulOuter < ulDisjuncts; ulOuter++)
	{
		CExpression *pexprDisj = (*pexpr)[ulOuter];
		if (CPredicateUtils::FAnd(pexprDisj))
		{
			DrgPexpr *pdrgpexprConjuncts = GPOS_NEW(pmp) DrgPexpr(pmp);
			const ULONG ulSize = pexprDisj->UlArity();
			for (ULONG ulInner = 0; ulInner < ulSize; ulInner++)
			{
				CExpression *pexprConj = (*pexprDisj)[ulInner];
				AddFactor(pmp, pexprConj, pdrgpexprFactors, pdrgpexprConjuncts, pexprmapFactors, ulDisjuncts);
			}

			if (0 < pdrgpexprConjuncts->UlLength())
			{
				pdrgpexprResidual->Append(CPredicateUtils::PexprConjunction(pmp, pdrgpexprConjuncts));
			}
			else
			{
				pdrgpexprConjuncts->Release();
			}
		}
		else
		{
			AddFactor(pmp, pexprDisj, pdrgpexprFactors, pdrgpexprResidual, pexprmapFactors, ulDisjuncts);
		}
	}
	pexprmapFactors->Release();

	if (0 < pdrgpexprResidual->UlLength())
	{
		// residual becomes a new factor
		pdrgpexprFactors->Append(CPredicateUtils::PexprDisjunction(pmp, pdrgpexprResidual));
	}
	else
	{
		// no residuals, release created array
		pdrgpexprResidual->Release();
	}

	// return a conjunction of all factors
	return CPredicateUtils::PexprConjunction(pmp, pdrgpexprFactors);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprDiscoverFactors
//
//	@doc:
//		Discover common factors in scalar expressions
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprDiscoverFactors
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	return PexprProcessDisjDescendents(pmp, pexpr, NULL /*pexprLowestLogicalAncestor*/, PexprFactorizeDisj);
}


//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprFactorize
//
//	@doc:
//		Factorize common scalar expressions
//
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprFactorize
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	CExpression *pexprFactorized = PexprDiscoverFactors(pmp, pexpr);

	// factorization might reveal unnested AND/OR
	CExpression *pexprUnnested = CExpressionUtils::PexprUnnest(pmp, pexprFactorized);
	pexprFactorized->Release();

	// eliminate duplicate AND/OR children
	CExpression *pexprDeduped = CExpressionUtils::PexprDedupChildren(pmp, pexprUnnested);
	pexprUnnested->Release();

	return pexprDeduped;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PcrsUsedByPushableScalar
//
//	@doc:
//		If the given expression is a scalar that can be pushed, it returns
//		the set of used columns.
//
//---------------------------------------------------------------------------
CColRefSet*
CExpressionFactorizer::PcrsUsedByPushableScalar
	(
	CExpression *pexpr
	)
{
	if (!pexpr->Pop()->FScalar())
	{
		return NULL;
	}

	CDrvdPropScalar *pdpscalar = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive());
	if (0 < pdpscalar->PcrsDefined()->CElements() ||
	    pdpscalar->FHasSubquery() ||
	    IMDFunction::EfsVolatile == pdpscalar->Pfp()->Efs() ||
	    IMDFunction::EfdaNoSQL != pdpscalar->Pfp()->Efda())
	{
		return NULL;
	}

	return pdpscalar->PcrsUsed();
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::FOpSourceIdOrComputedColumn
//
//	@doc:
//		If the given expression is a non volatile scalar expression using table columns
//		created by the same operator (or using the same computed column)
//		return true and set ulOpSourceId to the id of the operator that created those
//		columns (set *ppcrComputedColumn to the computed column used),
//		otherwise return false.
//
//---------------------------------------------------------------------------
BOOL
CExpressionFactorizer::FOpSourceIdOrComputedColumn
	(
	CExpression *pexpr,
	ULONG *ulOpSourceId,
	CColRef **ppcrComputedColumn
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != ulOpSourceId);
	GPOS_ASSERT(NULL != ppcrComputedColumn);

	*ulOpSourceId = gpos::ulong_max;
	*ppcrComputedColumn = NULL;

	CColRefSet* pcrsUsed = PcrsUsedByPushableScalar(pexpr);
	if (NULL == pcrsUsed || 0 == pcrsUsed->CElements())
	{
		return false;
	}

	ULONG ulComputedOpSourceId = gpos::ulong_max;
	CColRefSetIter crsi(*pcrsUsed);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		if (CColRef::EcrtTable != pcr->Ecrt())
		{
			if (NULL == *ppcrComputedColumn)
			{
				*ppcrComputedColumn = pcr;
			}
			else if (pcr != *ppcrComputedColumn)
			{
				return false;
			}

			continue;
		}
		else if (NULL != *ppcrComputedColumn)
		{
			// don't allow a mix of computed columns and table columns
			return false;
		}

		const CColRefTable *pcrTable = CColRefTable::PcrConvert(pcr);
		if (gpos::ulong_max == ulComputedOpSourceId)
		{
			ulComputedOpSourceId = pcrTable->UlSourceOpId();
		}
		else if (ulComputedOpSourceId != pcrTable->UlSourceOpId())
		{
			// expression refers to columns coming from different operators
			return false;
		}
	}
	*ulOpSourceId = ulComputedOpSourceId;

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForSourceId
//
//	@doc:
//		Find the array of expression arrays corresponding to the given
//		operator source id in the given source to array position map
//		or construct a new array and add it to the map.
//
//---------------------------------------------------------------------------
DrgPdrgPexpr *
CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForSourceId
	(
	IMemoryPool *pmp,
	SourceToArrayPosMap *psrc2array,
	BOOL fAllowNewSources,
	ULONG ulOpSourceId
	)
{
	GPOS_ASSERT(NULL != psrc2array);
	DrgPdrgPexpr *pdrgpdrgpexpr = psrc2array->PtLookup(&ulOpSourceId);

	// if there is no entry, we start recording expressions that will become disjuncts
	// corresponding to the source operator we are considering
	if (NULL == pdrgpdrgpexpr)
	{
		// checking this flag allows us to disable adding new entries: if a source operator
		// does not appear in the first disjunct, there is no need to add it later since it
		// will not cover the entire disjunction
		if (!fAllowNewSources)
		{
			return NULL;
		}
		pdrgpdrgpexpr = GPOS_NEW(pmp) DrgPdrgPexpr(pmp);
	#ifdef GPOS_DEBUG
		BOOL fInserted =
	#endif // GPOS_DEBUG
		psrc2array->FInsert(GPOS_NEW(pmp) ULONG(ulOpSourceId), pdrgpdrgpexpr);
		GPOS_ASSERT(fInserted);
	}

	return pdrgpdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForColumn
//
//	@doc:
// 		Find the array of expression arrays corresponding to the given
// 		column in the given column to array position map
// 		or construct a new array and add it to the map.
//
//---------------------------------------------------------------------------
DrgPdrgPexpr *
CExpressionFactorizer::PdrgPdrgpexprDisjunctArrayForColumn
	(
	IMemoryPool *pmp,
	ColumnToArrayPosMap *pcol2array,
	BOOL fAllowNewSources,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pcol2array);
	DrgPdrgPexpr *pdrgpdrgpexpr = pcol2array->PtLookup(pcr);

	// if there is no entry, we start recording expressions that will become disjuncts
	// corresponding to the computed column we are considering
	if (NULL == pdrgpdrgpexpr)
	{
		// checking this flag allows us to disable adding new entries: if a column
		// does not appear in the first disjunct, there is no need to add it later since it
		// will not cover the entire disjunction
		if (!fAllowNewSources)
		{
			return NULL;
		}
		pdrgpdrgpexpr = GPOS_NEW(pmp) DrgPdrgPexpr(pmp);
	#ifdef GPOS_DEBUG
		BOOL fInserted =
	#endif // GPOS_DEBUG
		pcol2array->FInsert(pcr, pdrgpdrgpexpr);
		GPOS_ASSERT(fInserted);
	}

	return pdrgpdrgpexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::RecordComparison
//
//	@doc:
//		If the given expression is a table column to constant comparison,
//		record it in the 'psrc2array' map.
//		If 'fAllowNewSources' is false, no new entries can be created in the
//		map. 'ulPosition' indicates the position in the entry where to add
//		the expression.
//
//---------------------------------------------------------------------------
void
CExpressionFactorizer::StoreBaseOpToColumnExpr
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	SourceToArrayPosMap *psrc2array,
	ColumnToArrayPosMap *pcol2array,
	const CColRefSet *pcrsProducedByChildren,
	BOOL fAllowNewSources,
	ULONG ulPosition
	)
{
	ULONG ulOpSourceId;
	CColRef *pcrComputed = NULL;
	if (!FOpSourceIdOrComputedColumn(pexpr, &ulOpSourceId, &pcrComputed))
	{
		return;
	}

	DrgPdrgPexpr *pdrgpdrgpexpr = NULL;

	if (gpos::ulong_max != ulOpSourceId)
	{
		pdrgpdrgpexpr = PdrgPdrgpexprDisjunctArrayForSourceId
						(
						pmp,
						psrc2array,
						fAllowNewSources,
						ulOpSourceId
						);
	}
	else
	{
		GPOS_ASSERT(NULL != pcrComputed);
		if (NULL != pcrsProducedByChildren && pcrsProducedByChildren->FMember(pcrComputed))
		{
			// do not create filters for columns produced by the scalar tree of
			// a logical operator immediately under the current logical operator
			return;
		}

		pdrgpdrgpexpr = PdrgPdrgpexprDisjunctArrayForColumn
						(
						pmp,
						pcol2array,
						fAllowNewSources,
						pcrComputed
						);
	}

	if (NULL == pdrgpdrgpexpr)
	{
		return;
	}

	DrgPexpr *pdrgpexpr = NULL;
	// there are only two cases we need to consider
	// the first one is that we found the current source operator in all previous disjuncts
	// and now we are starting a new sub-array for a new disjunct
	if (ulPosition == pdrgpdrgpexpr->UlLength())
	{
		pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		pdrgpdrgpexpr->Append(pdrgpexpr);
	}
	// the second case is that we found additional conjuncts for the current source operator
	// inside the current disjunct
	else if (ulPosition == pdrgpdrgpexpr->UlLength() - 1)
	{
		pdrgpexpr = (*pdrgpdrgpexpr)[ulPosition];
	}
	// otherwise, this source operator is not covered by all disjuncts, so there is no need to
	// keep recording it since it will not lead to a valid pre-filter
	else
	{
		return;
	}

	pexpr->AddRef();
	pdrgpexpr->Append(pexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprAddInferredFilters
//
//	@doc:
//		Create a conjunction of the given expression and inferred filters constructed out
//		of the given map.
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprAddInferredFilters
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	SourceToArrayPosMap *psrc2array,
	ColumnToArrayPosMap *pcol2array
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr));
	GPOS_ASSERT(NULL != psrc2array);

	SourceToArrayPosMapIter src2arrayIter(psrc2array);
	DrgPexpr *pdrgpexprPrefilters = GPOS_NEW(pmp) DrgPexpr(pmp);
	pexpr->AddRef();
	pdrgpexprPrefilters->Append(pexpr);
	const ULONG ulDisjChildren = pexpr->UlArity();

	while (src2arrayIter.FAdvance())
	{
		AddInferredFiltersFromArray(pmp, src2arrayIter.Pt(), ulDisjChildren, pdrgpexprPrefilters);
	}

	ColumnToArrayPosMapIter col2arrayIter(pcol2array);
	while (col2arrayIter.FAdvance())
	{
		AddInferredFiltersFromArray(pmp, col2arrayIter.Pt(), ulDisjChildren, pdrgpexprPrefilters);
	}

	return CPredicateUtils::PexprConjunction(pmp, pdrgpexprPrefilters);
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprAddInferredFilters
//
//	@doc:
// 		Construct a filter based on the expressions from 'pdrgpdrgpexpr'
// 		and add to the array 'pdrgpexprPrefilters'.
//
//---------------------------------------------------------------------------
void
CExpressionFactorizer::AddInferredFiltersFromArray
	(
	IMemoryPool *pmp,
	const DrgPdrgPexpr *pdrgpdrgpexpr,
	ULONG ulDisjChildrenLength,
	DrgPexpr *pdrgpexprInferredFilters
	)
{
	const ULONG ulEntryLength = (pdrgpdrgpexpr == NULL) ? 0 : pdrgpdrgpexpr->UlLength();
	if (ulEntryLength == ulDisjChildrenLength)
	{
		DrgPexpr *pdrgpexprDisjuncts = GPOS_NEW(pmp) DrgPexpr(pmp);
		for (ULONG ul = 0; ul < ulEntryLength; ++ul)
		{
			(*pdrgpdrgpexpr)[ul]->AddRef();
			CExpression *pexprConj =
					CPredicateUtils::PexprConjunction(pmp, (*pdrgpdrgpexpr)[ul]);
			pdrgpexprDisjuncts->Append(pexprConj);
		}
		if (0 < pdrgpexprDisjuncts->UlLength())
		{
			pdrgpexprInferredFilters->Append(
					CPredicateUtils::PexprDisjunction(pmp, pdrgpexprDisjuncts));
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PcrsColumnsProducedByChildren
//
//	@doc:
//		Returns the set of columns produced by the scalar trees of the given expression's
//		children
//
//---------------------------------------------------------------------------
CColRefSet *
CExpressionFactorizer::PcrsColumnsProducedByChildren
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	const ULONG ulArity = pexpr->UlArity();
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	for (ULONG ulTop = 0; ulTop < ulArity; ulTop++)
	{
		CExpression *pexprChild = (*pexpr)[ulTop];
		const ULONG ulChildArity = pexprChild->UlArity();
		for (ULONG ulBelowChild = 0; ulBelowChild < ulChildArity; ulBelowChild++)
		{
			CExpression *pexprGrandChild = (*pexprChild)[ulBelowChild];
			if (pexprGrandChild->Pop()->FScalar())
			{
				CColRefSet *pcrsChildDefined =
						CDrvdPropScalar::Pdpscalar(pexprGrandChild->PdpDerive())->PcrsDefined();
				pcrs->Include(pcrsChildDefined);
			}
		}
	}

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprExtractInferredFiltersFromDisj
//
//	@doc:
//		Compute disjunctive inferred filters that can be pushed to the column creators
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprExtractInferredFiltersFromDisj
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CExpression *pexprLowestLogicalAncestor
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CPredicateUtils::FOr(pexpr) && "input must be an OR expression");
	GPOS_ASSERT(NULL != pexprLowestLogicalAncestor);

	const ULONG ulArity = pexpr->UlArity();
	GPOS_ASSERT(2 <= ulArity);

	// for each source operator, create a map entry which, for every disjunct,
	// has the array of comparisons using that operator
	// we initialize the entries with operators appearing in the first disjunct
	SourceToArrayPosMap *psrc2array = GPOS_NEW(pmp) SourceToArrayPosMap(pmp);

	// create a similar map for computed columns
	ColumnToArrayPosMap *pcol2array = GPOS_NEW(pmp) ColumnToArrayPosMap(pmp);

	CColRefSet *pcrsProducedByChildren = NULL;
	if (COperator::EopLogicalSelect == pexprLowestLogicalAncestor->Pop()->Eopid())
	{
		pcrsProducedByChildren = PcrsColumnsProducedByChildren(pmp, pexprLowestLogicalAncestor);
	}

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprCurrent = (*pexpr)[ul];
		BOOL fFirst = 0 == ul;
		if (CPredicateUtils::FAnd(pexprCurrent))
		{
			const ULONG ulAndArity = pexprCurrent->UlArity();
			for (ULONG ulAnd = 0; ulAnd < ulAndArity; ++ulAnd)
			{
				StoreBaseOpToColumnExpr
					(
					pmp,
					(*pexprCurrent)[ulAnd],
					psrc2array,
					pcol2array,
					pcrsProducedByChildren,
					fFirst,  // fAllowNewSources
					ul
					);
			}
		}
		else
		{
			StoreBaseOpToColumnExpr
				(
				pmp,
				pexprCurrent,
				psrc2array,
				pcol2array,
				pcrsProducedByChildren,
				fFirst /*fAllowNewSources*/,
				ul
				);
		}

		if (fFirst && 0 == psrc2array->UlEntries() && 0 == pcol2array->UlEntries())
		{
			psrc2array->Release();
			pcol2array->Release();
			CRefCount::SafeRelease(pcrsProducedByChildren);
			pexpr->AddRef();
			return pexpr;
		}
		GPOS_CHECK_ABORT;
	}
	CExpression *pexprWithPrefilters = PexprAddInferredFilters(pmp, pexpr, psrc2array, pcol2array);
	psrc2array->Release();
	pcol2array->Release();
	CRefCount::SafeRelease(pcrsProducedByChildren);
	CExpression *pexprDeduped = CExpressionUtils::PexprDedupChildren(pmp, pexprWithPrefilters);
	pexprWithPrefilters->Release();

	return pexprDeduped;
}

//---------------------------------------------------------------------------
//	@function:
//		CExpressionFactorizer::PexprExtractInferredFilters
//
//	@doc:
//		Compute disjunctive pre-filters that can be pushed to the column creators.
//		These inferred filters need to "cover" all the disjuncts with expressions
//		coming from the same source.
//
//		For instance, out of the predicate
//		((sale_type = 's'::text AND dyear = 2001 AND year_total > 0::numeric) OR
//		(sale_type = 's'::text AND dyear = 2002) OR
//		(sale_type = 'w'::text AND dmoy = 7 AND year_total > 0::numeric) OR
//		(sale_type = 'w'::text AND dyear = 2002 AND dmoy = 7))
//
//		we can infer the filter
//		dyear=2001 OR dyear=2002 OR dmoy=7 OR (dyear=2002 AND dmoy=7)
//
//		which can later be pushed down by the normalizer
//
//---------------------------------------------------------------------------
CExpression *
CExpressionFactorizer::PexprExtractInferredFilters
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	return PexprProcessDisjDescendents
	  (
	   pmp,
	   pexpr,
	   NULL /*pexprLowestLogicalAncestor*/,
	   PexprExtractInferredFiltersFromDisj
	  );
}


// EOF
