//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal Inc.
//
//	@filename:
//		CXformCollapseProject.cpp
//
//	@doc:
//		Implementation of the transform that collapses two cascaded project nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformCollapseProject.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseProject::CXformCollapseProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformCollapseProject::CXformCollapseProject
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
					GPOS_NEW(pmp) CLogicalProject(pmp),
					GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalProject(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
						),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
					)
		)
{}



//---------------------------------------------------------------------------
//	@function:
//		CXformSplitDQA::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformCollapseProject::Exfp
	(
	CExpressionHandle &//exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformCollapseProject::Transform
//
//	@doc:
//		Actual transformation to collapse projects
//
//---------------------------------------------------------------------------
void
CXformCollapseProject::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	CExpression *pexprCollapsed = CUtils::PexprCollapseProjects(pmp, pexpr);

	if (NULL != pexprCollapsed)
	{
		pxfres->Add(pexprCollapsed);
	}
}

// EOF
