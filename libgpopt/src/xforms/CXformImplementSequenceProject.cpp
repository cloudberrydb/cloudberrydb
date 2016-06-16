//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementSequenceProject.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformImplementSequenceProject.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSequenceProject::CXformImplementSequenceProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementSequenceProject::CXformImplementSequenceProject
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalSequenceProject(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // scalar child
						)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementSequenceProject::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementSequenceProject::Transform
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
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// addref all children
	pexprRelational->AddRef();
	pexprScalar->AddRef();

	// extract members of logical sequence project operator
	CLogicalSequenceProject *popLogicalSequenceProject = CLogicalSequenceProject::PopConvert(pexpr->Pop());
	CDistributionSpec *pds = popLogicalSequenceProject->Pds();
	DrgPos *pdrgpos = popLogicalSequenceProject->Pdrgpos();
	DrgPwf *pdrgpwf = popLogicalSequenceProject->Pdrgpwf();
	pds->AddRef();
	pdrgpos->AddRef();
	pdrgpwf->AddRef();

	// assemble physical operator
	CExpression *pexprSequenceProject =
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CPhysicalSequenceProject(pmp, pds, pdrgpos, pdrgpwf),
					pexprRelational,
					pexprScalar
					);

	// add alternative to results
	pxfres->Add(pexprSequenceProject);
}


// EOF

