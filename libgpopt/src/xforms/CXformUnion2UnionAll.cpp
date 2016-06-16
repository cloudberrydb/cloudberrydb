//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUnion2UnionAll.cpp
//
//	@doc:
//		Implementation of the transformation that takes a logical union and
//		coverts it into an aggregate over a logical union all
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformUnion2UnionAll.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformUnion2UnionAll::CXformUnion2UnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUnion2UnionAll::CXformUnion2UnionAll
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalUnion(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp))
						)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformUnion2UnionAll::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformUnion2UnionAll::Transform
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

	// extract components
	CLogicalUnion *popUnion = CLogicalUnion::PopConvert(pexpr->Pop());
	DrgPcr *pdrgpcrOutput = popUnion->PdrgpcrOutput();
	DrgDrgPcr *pdrgpdrgpcrInput = popUnion->PdrgpdrgpcrInput();

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexpr->UlArity();

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	pdrgpcrOutput->AddRef();
	pdrgpdrgpcrInput->AddRef();

	// assemble new logical operator
	CExpression *pexprUnionAll = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrOutput, pdrgpdrgpcrInput),
									pdrgpexpr
									);

	pdrgpcrOutput->AddRef();

	CExpression *pexprProjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), GPOS_NEW(pmp) DrgPexpr(pmp));

	CExpression *pexprAgg = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcrOutput, COperator::EgbaggtypeGlobal /*egbaggtype*/),
										pexprUnionAll,
										pexprProjList
										);

	// add alternative to results
	pxfres->Add(pexprAgg);
}

// EOF
