//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformJoinAssociativity.cpp
//
//	@doc:
//		Implementation of associativity transform for left-deep joins
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformJoinAssociativity.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpnaucrates;

#define GPOPT_MAX_JOIN_DEPTH_FOR_ASSOCIATIVITY	20
#define GPOPT_MAX_JOIN_RIGHT_CHILD_DEPTH_FOR_ASSOCIATIVITY 1

//---------------------------------------------------------------------------
//	@function:
//		CXformJoinAssociativity::CXformJoinAssociativity
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformJoinAssociativity::CXformJoinAssociativity
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
					GPOS_NEW(pmp) CExpression  // left child is a join tree
					 	 (
					 	 pmp,
					 	 GPOS_NEW(pmp) CLogicalInnerJoin(pmp),
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
					 	 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // predicate
					 	 ),
					GPOS_NEW(pmp) CExpression // right child is a pattern leaf
								(
								pmp,
								GPOS_NEW(pmp) CPatternLeaf(pmp)
								),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp)) // join predicate
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformJoinAssociativity::CreatePredicates
//
//	@doc:
//		Extract all conjuncts and divvy them up between upper and lower join
//
//---------------------------------------------------------------------------
void
CXformJoinAssociativity::CreatePredicates
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPexpr *pdrgpexprLower,
	DrgPexpr *pdrgpexprUpper
	)
	const
{
	GPOS_CHECK_ABORT;

	// bind operators
	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprLeftLeft = (*pexprLeft)[0];
	CExpression *pexprRight = (*pexpr)[1];
	
	DrgPexpr *pdrgpexprJoins = GPOS_NEW(pmp) DrgPexpr(pmp);

	pexprLeft->AddRef();
	pdrgpexprJoins->Append(pexprLeft);
	
	pexpr->AddRef();
	pdrgpexprJoins->Append(pexpr);	
	
	// columns for new lower join
	CColRefSet *pcrsLower = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsLower->Union(CDrvdPropRelational::Pdprel(pexprLeftLeft->PdpDerive())->PcrsOutput());
	pcrsLower->Union(CDrvdPropRelational::Pdprel(pexprRight->PdpDerive())->PcrsOutput());
	
	// convert current predicates into arrays of conjuncts
	DrgPexpr *pdrgpexprOrig = GPOS_NEW(pmp) DrgPexpr(pmp);
	
	for (ULONG ul = 0; ul < 2; ul++)
	{
		DrgPexpr *pdrgpexprPreds = CPredicateUtils::PdrgpexprConjuncts(pmp, (*(*pdrgpexprJoins)[ul])[2]);
		ULONG ulLen = pdrgpexprPreds->UlLength();
		for (ULONG ulConj = 0; ulConj < ulLen; ulConj++)
		{	
			CExpression *pexprConj = (*pdrgpexprPreds)[ulConj];
			pexprConj->AddRef();
			
			pdrgpexprOrig->Append(pexprConj);
		}
		pdrgpexprPreds->Release();
	}

	// divvy up conjuncts for upper and lower join
	ULONG ulConj = pdrgpexprOrig->UlLength();
	for (ULONG ul = 0; ul < ulConj; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprOrig)[ul];
		CColRefSet *pcrs = CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();
		
		pexprPred->AddRef();
		if (pcrsLower->FSubset(pcrs))
		{
			pdrgpexprLower->Append(pexprPred);
		}
		else 
		{
			pdrgpexprUpper->Append(pexprPred);			
		}
	}
	
	// No predicates indicate a cross join. And for that, ORCA expects
	// predicate to be a scalar const "true".
	if (pdrgpexprLower->UlLength() == 0)
	{
		CExpression *pexprCrossLowerJoinPred = CUtils::PexprScalarConstBool(pmp, true, false);
		pdrgpexprLower->Append(pexprCrossLowerJoinPred);
	}
	
	// Same for upper predicates
	if (pdrgpexprUpper->UlLength() == 0)
	{
		CExpression *pexprCrossUpperJoinPred = CUtils::PexprScalarConstBool(pmp, true, false);
		pdrgpexprUpper->Append(pexprCrossUpperJoinPred);
	}

	// clean up
	pcrsLower->Release();
	pdrgpexprOrig->Release();
	
	pdrgpexprJoins->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CXformJoinAssociativity::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformJoinAssociativity::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if 
		(
		GPOPT_MAX_JOIN_DEPTH_FOR_ASSOCIATIVITY < exprhdl.Pdprel()->UlJoinDepth() ||  // disallow xform beyond max join depth
		GPOPT_MAX_JOIN_RIGHT_CHILD_DEPTH_FOR_ASSOCIATIVITY < exprhdl.Pdprel(1)->UlJoinDepth()  // disallow xform if input is not a left deep tree
		)
	{
		// restrict associativity to left-deep trees by prohibiting the
		// transformation when right child's join depth is above threshold
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//	Associativity Transform: (RS)T ==> (RT)S
//	Example:
//	Input Expression:
//	+--CLogicalInnerJoin
//		 |--CLogicalInnerJoin
//		 |  |--CLogicalGet "t1"
//		 |  |--CLogicalGet "t2"
//		 |  +--CScalarCmp (=)
//		 |     |--CScalarIdent "a" (0) ==> from t1
//		 |     +--CScalarIdent "b" (9) ==> from t2
//		 |--CLogicalGet "t3"
//		 +--CScalarCmp (=)
//				|--CScalarIdent "a" (0) ==> from t1
//				+--CScalarIdent "c" (19) ==> from t3
//
//	Output CExpression:
//	+--CLogicalInnerJoin
//		 |--CLogicalInnerJoin
//		 |  |--CLogicalGet "t1"
//		 |  |--CLogicalGet "t3"
//		 |  +--CScalarCmp (=)
//		 |     |--CScalarIdent "a" (0)  ==> from t1
//		 |     +--CScalarIdent "c" (19) ==> from t3
//		 |--CLogicalGet "t2"
//		 +--CScalarCmp (=)
//				|--CScalarIdent "a" (0) ==> from t1
//				+--CScalarIdent "b" (9) ==> from t2

void
CXformJoinAssociativity::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();
	
	// create new predicates
	DrgPexpr *pdrgpexprLower = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprUpper = GPOS_NEW(pmp) DrgPexpr(pmp);
	CreatePredicates(pmp, pexpr, pdrgpexprLower, pdrgpexprUpper);
	
	GPOS_ASSERT(pdrgpexprLower->UlLength() > 0);
	
	//  cross join contains CScalarConst(1) as the join condition.  if the
	//  input expression is as below with cross join at top level between
	//  CLogicalInnerJoin and CLogicalGet "t3"
	//  +--CLogicalInnerJoin
	//     |--CLogicalInnerJoin
	//     |  |--CLogicalGet "t1"
	//     |  |--CLogicalGet "t2"
	//     |  +--CScalarCmp (=)
	//     |     |--CScalarIdent "a" (0)
	//     |     +--CScalarIdent "b" (9)
	//     |--CLogicalGet "t3"
	//     +--CScalarConst (1)
	//  for the above expression (lower) predicate generated for the cross join
	//  between t1 and t3 will be: CScalarConst (1) In *only* such cases, donot
	//  generate such alternative with the lower join as cross join example:
	//  +--CLogicalInnerJoin
	//     |--CLogicalInnerJoin
	//     |  |--CLogicalGet "t1"
	//     |  |--CLogicalGet "t3"
	//     |  +--CScalarConst (1)
	//     |--CLogicalGet "t2"
	//     +--CScalarCmp (=)
	//        |--CScalarIdent "a" (0)
	//        +--CScalarIdent "b" (9)

	// NOTE that we want to be careful to check that input lower join wasn't a
	// cross join to begin with, because we want to build a join in this case even
	// though a new cross join will be created.

	// check if the input lower join expression is a cross join
	BOOL fInputLeftIsCrossJoin = CUtils::FCrossJoin((*pexpr)[0]);

	// check if the output lower join would result in a cross join
	BOOL fOutputLeftIsCrossJoin = (1 == pdrgpexprLower->UlLength() &&
			CUtils::FScalarConstTrue((*pdrgpexprLower)[0]));

	// build a join only if it does not result in a cross join
	// unless the input itself was a cross join (see earlier comments)
	if (!fOutputLeftIsCrossJoin || fInputLeftIsCrossJoin)
	{
		// bind operators
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprLeftLeft = (*pexprLeft)[0];
		CExpression *pexprLeftRight = (*pexprLeft)[1];
		CExpression *pexprRight = (*pexpr)[1];

		// add-ref all components for re-use
		pexprLeftLeft->AddRef();
		pexprRight->AddRef();
		pexprLeftRight->AddRef();
		
		// build new joins
		CExpression *pexprBottomJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>
										(
										pmp,
										pexprLeftLeft,
										pexprRight,
										CPredicateUtils::PexprConjunction(pmp, pdrgpexprLower)
										);

		CExpression *pexprResult = CUtils::PexprLogicalJoin<CLogicalInnerJoin>
									(
									pmp,
									pexprBottomJoin,
									pexprLeftRight,
									CPredicateUtils::PexprConjunction(pmp, pdrgpexprUpper)
									);

		// add alternative to transformation result
		pxfres->Add(pexprResult);
	}
	else
	{
		pdrgpexprLower->Release();
		pdrgpexprUpper->Release();
	}
}


// EOF

