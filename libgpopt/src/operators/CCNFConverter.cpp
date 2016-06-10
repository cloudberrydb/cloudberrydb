//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2012 EMC Corp.
//
//	@filename:
//		CCNFConverter.cpp
//
//	@doc:
//		Implementation of CNF converter
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CCNFConverter.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarBoolOp.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::FScalarBoolOp
//
//	@doc:
//		Does the given expression have a scalar bool op?
//
//---------------------------------------------------------------------------
BOOL
CCNFConverter::FScalarBoolOp
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	return COperator::EopScalarBoolOp == pexpr->Pop()->Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::PdrgpexprConvertChildren
//
//	@doc:
//		Call converter recursively on children array
//
//---------------------------------------------------------------------------
DrgPexpr *
CCNFConverter::PdrgpexprConvertChildren
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	const ULONG ulArity = pexpr->UlArity();
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = Pexpr2CNF(pmp, (*pexpr)[ul]);
		pdrgpexpr->Append(pexprChild);
	}

	return pdrgpexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::Pdrgpdrgpexpr
//
//	@doc:
//		Create an array of arrays each holding the children of an expression
//		from the given array
//
//
//---------------------------------------------------------------------------
DrgPdrgPexpr *
CCNFConverter::Pdrgpdrgpexpr
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpexpr);

	DrgPdrgPexpr *pdrgpdrgpexpr = GPOS_NEW(pmp) DrgPdrgPexpr(pmp);
	const ULONG ulArity = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		DrgPexpr *pdrgpexprChild = NULL;

		if (CPredicateUtils::FAnd(pexpr))
		{
			pdrgpexprChild = pexpr->PdrgPexpr();
			pdrgpexprChild->AddRef();
		}
		else
		{
			pdrgpexprChild = GPOS_NEW(pmp) DrgPexpr(pmp);
			pexpr->AddRef();
			pdrgpexprChild->Append(pexpr);
		}
		pdrgpdrgpexpr->Append(pdrgpexprChild);
	}

	return pdrgpdrgpexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::Expand
//
//	@doc:
//		Recursively compute cross product over input array of arrays;
//		convert each cross product result to a conjunct and store it in
//		output array
//
//---------------------------------------------------------------------------
void
CCNFConverter::Expand
	(
	IMemoryPool *pmp,
	DrgPdrgPexpr *pdrgpdrgpexprInput, // input array of arrays
	DrgPexpr *pdrgpexprOutput, // output array
	DrgPexpr *pdrgpexprToExpand, // a cross product result to be expanded
	ULONG ulCurrent // index of current array to be used for expanding a cross product result
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(ulCurrent <= pdrgpdrgpexprInput->UlLength());

	if (ulCurrent == pdrgpdrgpexprInput->UlLength())
	{
		// exhausted input, add a new conjunct to output array
		pdrgpexprOutput->Append(CPredicateUtils::PexprDisjunction(pmp, pdrgpexprToExpand));

		return;
	}

	DrgPexpr *pdrgpexprCurrent = (*pdrgpdrgpexprInput)[ulCurrent];
	const ULONG ulArity = pdrgpexprCurrent->UlLength();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		// append accumulated expressions to a new array
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
		CUtils::AddRefAppend<CExpression, CleanupRelease>(pdrgpexpr, pdrgpexprToExpand);

		// add an expression from current input array only if it's not equal to an existing one
		if (!CUtils::FEqualAny((*pdrgpexprCurrent)[ul], pdrgpexpr))
		{
			CExpression *pexpr = (*pdrgpexprCurrent)[ul];
			pexpr->AddRef();
			pdrgpexpr->Append(pexpr);
		}

		// recursively expand the resulting array
		Expand(pmp, pdrgpdrgpexprInput, pdrgpexprOutput, pdrgpexpr, ulCurrent + 1);
	}
	pdrgpexprToExpand->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::PexprAnd2CNF
//
//	@doc:
//		Convert an AND tree into CNF
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverter::PexprAnd2CNF
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	DrgPexpr *pdrgpexprChildren = PdrgpexprConvertChildren(pmp, pexpr);
	return CPredicateUtils::PexprConjunction(pmp, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::PexprOr2CNF
//
//	@doc:
//		Convert an OR tree into CNF
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverter::PexprOr2CNF
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	DrgPexpr *pdrgpexpr = PdrgpexprConvertChildren(pmp, pexpr);

	// build array of arrays each representing terms in a disjunct
	DrgPdrgPexpr *pdrgpdrgpexprDisjuncts = Pdrgpdrgpexpr(pmp, pdrgpexpr);
	pdrgpexpr->Release();

	// compute conjuncts by distributing AND over OR
	DrgPexpr *pdrgpexprChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	Expand(pmp, pdrgpdrgpexprDisjuncts, pdrgpexprChildren, GPOS_NEW(pmp) DrgPexpr(pmp), 0 /*ulCurrent*/);
	pdrgpdrgpexprDisjuncts->Release();

	return CPredicateUtils::PexprConjunction(pmp, pdrgpexprChildren);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::PexprNot2CNF
//
//	@doc:
//		Convert a NOT tree into CNF
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverter::PexprNot2CNF
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(1 == pexpr->UlArity());

	CExpression *pexprNotChild = (*pexpr)[0];
	if (!FScalarBoolOp(pexprNotChild))
	{
		pexpr->AddRef();
		return pexpr;
	}

	CScalarBoolOp::EBoolOperator eboolopChild = CScalarBoolOp::PopConvert(pexprNotChild->Pop())->Eboolop();

	// apply DeMorgan laws

	// NOT(NOT(A)) ==> A
	if (CScalarBoolOp::EboolopNot == eboolopChild)
	{
		return Pexpr2CNF(pmp, (*pexprNotChild)[0]);
	}

	// Not child must be either an AND or an OR

	// NOT(A AND B) ==> NOT(A) OR  NOT(B)
	// NOT(A OR  B) ==> NOT(A) AND NOT(B)
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexprNotChild->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		(*pexprNotChild)[ul]->AddRef();
		pdrgpexpr->Append(CUtils::PexprNegate(pmp, (*pexprNotChild)[ul]));
	}

	CScalarBoolOp::EBoolOperator eboolop = CScalarBoolOp::EboolopAnd;
	if (CScalarBoolOp::EboolopAnd == eboolopChild)
	{
		eboolop = CScalarBoolOp::EboolopOr;
	}

	CExpression *pexprScalarBoolOp =  CUtils::PexprScalarBoolOp(pmp, eboolop, pdrgpexpr);
	CExpression *pexprResult = Pexpr2CNF(pmp, pexprScalarBoolOp);
	pexprScalarBoolOp->Release();

	return pexprResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverter::Pexpr2CNF
//
//	@doc:
//		Main driver
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverter::Pexpr2CNF
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	// protect against stack overflow during recursion
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	if (!FScalarBoolOp(pexpr))
	{
		pexpr->AddRef();
		return pexpr;
	}

	const CScalarBoolOp::EBoolOperator eboolop = CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop();
	switch (eboolop)
	{
		case CScalarBoolOp::EboolopAnd:
			return PexprAnd2CNF(pmp, pexpr);

		case CScalarBoolOp::EboolopOr:
			return PexprOr2CNF(pmp, pexpr);

		case CScalarBoolOp::EboolopNot:
			return PexprNot2CNF(pmp, pexpr);

		default:
			GPOS_ASSERT(!"Unexpected ScalarBoolOp");
			return NULL;
	}
}



// EOF
