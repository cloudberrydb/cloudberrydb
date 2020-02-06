//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalNAryJoin.cpp
//
//	@doc:
//		Implementation of n-ary inner join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::CLogicalNAryJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalNAryJoin::CLogicalNAryJoin
	(
	CMemoryPool *mp
	)
	:
	CLogicalJoin(mp),
	m_lojChildPredIndexes(NULL)
{
	GPOS_ASSERT(NULL != mp);
}

CLogicalNAryJoin::CLogicalNAryJoin
	(
	CMemoryPool *mp,
	ULongPtrArray *lojChildIndexes
	)
	:
	CLogicalJoin(mp),
	m_lojChildPredIndexes(lojChildIndexes)
{
	GPOS_ASSERT(NULL != mp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::DeriveMaxCard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalNAryJoin::DeriveMaxCard
	(
	CMemoryPool *mp,
	CExpressionHandle &exprhdl
	)
	const
{
	CMaxCard maxCard(1);
	const ULONG arity = exprhdl.Arity();

	// loop over the inner join logical children only
	for (ULONG ul = 0; ul < arity - 1; ul++)
	{
		CMaxCard childMaxCard = exprhdl.DeriveMaxCard(ul);

		if (IsInnerJoinChild(ul) || 1 <= childMaxCard.Ull())
		{
			maxCard *= childMaxCard;
		}
	}

	if (exprhdl.DerivePropertyConstraint()->FContradiction())
	{
		return CMaxCard(0 /*ull*/);
	}

	CExpression *pexprScalar = exprhdl.PexprScalarChild(arity-1);

	if (NULL != pexprScalar)
	{
		if (COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid())
		{
			CExpression *pexprScalarChild = GetTrueInnerJoinPreds(mp, exprhdl);

			// in case of a false condition (when the operator is non Inner Join)
			// maxcard should be zero
			if (CUtils::FScalarConstFalse(pexprScalarChild))
			{
				pexprScalarChild->Release();
				return CMaxCard(0 /*ull*/);
			}
			pexprScalarChild->Release();
		}
		else
		{
			return CLogical::Maxcard(exprhdl, exprhdl.Arity() - 1, maxCard);
		}
	}

	return maxCard;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalNAryJoin::PxfsCandidates
	(
	CMemoryPool *mp
	) 
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	
	(void) xform_set->ExchangeSet(CXform::ExfSubqNAryJoin2Apply);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoin);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinMinCard);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinDP);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinGreedy);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinDPv2);

	return xform_set;
}

CExpression*
CLogicalNAryJoin::GetTrueInnerJoinPreds(CMemoryPool *mp, CExpressionHandle &exprhdl) const
{
	// "true" inner join predicates are those that don't rely on any tables that are
	// right children of non-inner joins. Example:
	//
	// select ... from foo left outer join bar on foo.a=bar.a join jazz on foo.b=jazz.b and coalesce(bar.c,0) = jazz.c;
	//
	// coalesce(bar.c,0) = jazz.c is not a "true" inner join predicate, since it relies
	// on column bar.c, which comes from an LOJ and therefore may be NULL, even though
	// bar.c might have been created with a NOT NULL constraint. We don't want to use
	// such predicates in constraint derivation.
	ULONG arity = exprhdl.Arity();
	CExpression *pexprScalar =  exprhdl.PexprScalarChild(arity-1);

	if (!HasOuterJoinChildren())
	{
		// all inner joins, all the predicates are true inner join preds
		pexprScalar->AddRef();
		return pexprScalar;
	}

	CExpressionArray *predArray = NULL;
	CExpressionArray *trueInnerJoinPredArray =  GPOS_NEW(mp) CExpressionArray (mp);
	CExpression *innerJoinPreds = (*pexprScalar)[0];
	BOOL isAConjunction = CPredicateUtils::FAnd(innerJoinPreds);

	GPOS_ASSERT(COperator::EopScalarNAryJoinPredList == pexprScalar->Pop()->Eopid());

	// split the predicate into conjuncts and inspect those individually
	predArray = CPredicateUtils::PdrgpexprConjuncts(mp,innerJoinPreds);

	for (ULONG ul = 0; ul < predArray->Size(); ul++)
	{
		CExpression *pred = (*predArray)[ul];
		CColRefSet *predCols = pred->DeriveUsedColumns();
		BOOL addToPredArray = true;

		// check whether the predicate uses any ColRefs that come from a non-inner join child
		for (ULONG c=0; c<exprhdl.Arity()-1; c++)
		{
			if (0 < *(*m_lojChildPredIndexes)[c])
			{
				// this is a right child of a non-inner join
				CColRefSet *nijOutputCols = exprhdl.DeriveOutputColumns(c);

				if (predCols->FIntersects(nijOutputCols))
				{
					// this predicate refers to some columns from non-inner joins,
					// which may become NULL, even when the type of the column is NOT NULL,
					// so the predicate may not actually be FALSE constants in some cases
					addToPredArray = false;
					break;
				}
			}
		}

		if (addToPredArray)
		{
			pred->AddRef();
			trueInnerJoinPredArray->Append(pred);
		}
	}

	predArray->Release();
	if (0 == trueInnerJoinPredArray->Size())
	{
		trueInnerJoinPredArray->Release();
		return CUtils::PexprScalarConstBool(mp, true);
	}
	return CPredicateUtils::PexprConjDisj(mp, trueInnerJoinPredArray, isAConjunction);
}
//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalNAryJoin::OsPrint
(
 IOstream &os
 )
const
{
	os	<< SzId();

	if (NULL != m_lojChildPredIndexes)
	{
		// print out the indexes of the logical children that correspond to
		// the scalar child entries below the CScalarNAryJoinPredList
		os << " [";
		ULONG size = m_lojChildPredIndexes->Size();
		for (ULONG ul=0; ul < size; ul++)
		{
			if (0 < ul)
			{
				os << ", ";
			}
			os << *((*m_lojChildPredIndexes)[ul]);
		}
		os	<< "]";
	}

	return os;
}



// EOF

