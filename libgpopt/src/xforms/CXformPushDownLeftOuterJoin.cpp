//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformPushDownLeftOuterJoin.cpp
//
//	@doc:
//		Implementation of left outer join push down transformation
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CNormalizer.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformPushDownLeftOuterJoin.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformPushDownLeftOuterJoin::CXformPushDownLeftOuterJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformPushDownLeftOuterJoin::CXformPushDownLeftOuterJoin
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
					GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp),
					GPOS_NEW(pmp) CExpression		// outer child is an NAry-Join
						(
						pmp,
						GPOS_NEW(pmp) CLogicalNAryJoin(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp)),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// NAry-join predicate tree
						),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),	// inner child is a leaf
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// LOJ predicate tree
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformPushDownLeftOuterJoin::Exfp
//
//	@doc:
//		Xform promise
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformPushDownLeftOuterJoin::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2);
	if (COperator::EopScalarConst == pexprScalar->Pop()->Eopid())
	{
		return CXform::ExfpNone;
	}
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformPushDownLeftOuterJoin::Transform
//
//	@doc:
//		Transform LOJ whose outer child is an NAry-join to be a child
//		of NAry-join
//
//		Input:
//			LOJ (a=d)
//				|---NAry-Join (a=b) and (b=c)
//				|     |--A
//				|     |--B
//				|     +--C
//				+--D
//
//		Output:
//			  NAry-Join (a=b) and (b=c)
//				|--B
//				|--C
//				+--LOJ (a=d)
//					|--A
//					+--D
//
//---------------------------------------------------------------------------
void
CXformPushDownLeftOuterJoin::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	CExpression *pexprNAryJoin = (*pexpr)[0];
	CExpression *pexprLOJInnerChild = (*pexpr)[1];
	CExpression *pexprLOJScalarChild = (*pexpr)[2];

	CColRefSet *pcrsLOJUsed = CDrvdPropScalar::Pdpscalar(pexprLOJScalarChild->PdpDerive())->PcrsUsed();
	DrgPexpr *pdrgpexprLOJChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprNAryJoinChildren = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulArity = pexprNAryJoin->UlArity();
	CExpression *pexprNAryJoinScalarChild = (*pexprNAryJoin)[ulArity - 1];
	for (ULONG ul = 0 ; ul < ulArity - 1; ul++)
	{
		CExpression *pexprChild = (*pexprNAryJoin)[ul];
		CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexprChild->PdpDerive())->PcrsOutput();
		pexprChild->AddRef();
		if (!pcrsOutput->FDisjoint(pcrsLOJUsed))
		{
			pdrgpexprLOJChildren->Append(pexprChild);
		}
		else
		{
			pdrgpexprNAryJoinChildren->Append(pexprChild);
		}
	}

	CExpression *pexprLOJOuterChild = (*pdrgpexprLOJChildren)[0];
	if (1 < pdrgpexprLOJChildren->UlLength())
	{
		// collect all relations needed by LOJ outer side into a cross product,
		// normalization at the end of this function takes care of pushing NAry
		// join predicates down
		pdrgpexprLOJChildren->Append(CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/));
		pexprLOJOuterChild = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalNAryJoin(pmp), pdrgpexprLOJChildren);

		// reconstruct LOJ children and add only the created child
		pdrgpexprLOJChildren = GPOS_NEW(pmp) DrgPexpr(pmp);
		pdrgpexprLOJChildren->Append(pexprLOJOuterChild);
	}

	// continue with rest of LOJ inner and scalar children
	pexprLOJInnerChild->AddRef();
	pdrgpexprLOJChildren->Append(pexprLOJInnerChild);
	pexprLOJScalarChild->AddRef();
	pdrgpexprLOJChildren->Append(pexprLOJScalarChild);

	// build new LOJ
	CExpression *pexprLOJNew = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp), pdrgpexprLOJChildren);

	// add new NAry join children
	pdrgpexprNAryJoinChildren->Append(pexprLOJNew);
	pexprNAryJoinScalarChild->AddRef();
	pdrgpexprNAryJoinChildren->Append(pexprNAryJoinScalarChild);

	if (3 > pdrgpexprNAryJoinChildren->UlLength())
	{
		// xform must generate a valid NAry-join expression
		// for example, in the following case we end-up with the same input
		// expression, which should be avoided:
		//
		//	Input:
		//
		//    LOJ (a=c) and (b=c)
		//     |--NAry-Join (a=b)
		//     |   |--A
		//     |   +--B
		//     +--C
		//
		//	Output:
		//
		//	  NAry-Join (true)
		//      +--LOJ (a=c) and (b=c)
		//           |--NAry-Join (a=b)
		//           |    |--A
		//           |    +--B
		//           +--C

		pdrgpexprNAryJoinChildren->Release();
		return;
	}

	// create new NAry join
	CExpression *pexprNAryJoinNew = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalNAryJoin(pmp), pdrgpexprNAryJoinChildren);

	// normalize resulting expression and add it to xform results
	CExpression *pexprResult = CNormalizer::PexprNormalize(pmp, pexprNAryJoinNew);
	pexprNAryJoinNew->Release();

	pxfres->Add(pexprResult);
}

// EOF
