//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformCollapseGbAgg.cpp
//
//	@doc:
//		Implementation of collapsing two cascaded group-by operators
//		into a single group-by
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
#include "gpopt/operators/ops.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformCollapseGbAgg.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseGbAgg::CXformCollapseGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCollapseGbAgg::CXformCollapseGbAgg
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
					GPOS_NEW(pmp) CLogicalGbAgg(pmp),
					GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalGbAgg(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
						),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		GbAgg must be global and have non empty grouping columns
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCollapseGbAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (!popAgg->FGlobal() || 0 == popAgg->Pdrgpcr()->UlLength())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseGbAgg::Transform
//
//	@doc:
//		Actual transformation to collapse two cascaded group by operators;
//		if the top Gb grouping columns are subset of bottom Gb grouping
//		columns AND both Gb operators do not define agg functions, we can
//		remove the bottom group by operator
//
//
//---------------------------------------------------------------------------
void
CXformCollapseGbAgg::Transform
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

	// extract components
	CLogicalGbAgg *popTopGbAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	GPOS_ASSERT(0 < popTopGbAgg->Pdrgpcr()->UlLength());
	GPOS_ASSERT(popTopGbAgg->FGlobal());

	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprTopProjectList = (*pexpr)[1];

	CLogicalGbAgg *popBottomGbAgg = CLogicalGbAgg::PopConvert(pexprRelational->Pop());
	CExpression *pexprChild = (*pexprRelational)[0];
	CExpression *pexprBottomProjectList = (*pexprRelational)[1];

	if (!popBottomGbAgg->FGlobal())
	{
		// bottom GbAgg must be global to prevent xform from getting applied to splitted GbAggs
		return;
	}

	if (0 < pexprTopProjectList->UlArity() || 0 < pexprBottomProjectList->UlArity())
	{
		// exit if any of the Gb operators has an aggregate function
		return;
	}

#ifdef GPOS_DEBUG
	// for two cascaded GbAgg ops with no agg functions, top grouping
	// columns must be a subset of bottom grouping columns
	CColRefSet *pcrsTopGrpCols = GPOS_NEW(pmp) CColRefSet(pmp, popTopGbAgg->Pdrgpcr());
	CColRefSet *pcrsBottomGrpCols = GPOS_NEW(pmp) CColRefSet(pmp, popBottomGbAgg->Pdrgpcr());
	GPOS_ASSERT(pcrsBottomGrpCols->FSubset(pcrsTopGrpCols));

	pcrsTopGrpCols->Release();
	pcrsBottomGrpCols->Release();
#endif // GPOS_DEBUG

	pexprChild->AddRef();
	CExpression *pexprSelect = CUtils::PexprLogicalSelect(pmp, pexprChild, CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/));

	popTopGbAgg->AddRef();
	pexprTopProjectList->AddRef();
	CExpression *pexprGbAggNew = GPOS_NEW(pmp) CExpression(pmp, popTopGbAgg, pexprSelect, pexprTopProjectList);

	pxfres->Add(pexprGbAggNew);
}


// EOF
