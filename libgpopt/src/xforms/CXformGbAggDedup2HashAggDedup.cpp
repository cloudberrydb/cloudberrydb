//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformGbAggDedup2HashAggDedup.cpp
//
//	@doc:
//		Implementation of transform
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

#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/xforms/CXformGbAggDedup2HashAggDedup.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggDedup2HashAggDedup::CXformGbAggDedup2HashAggDedup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAggDedup2HashAggDedup::CXformGbAggDedup2HashAggDedup
	(
	IMemoryPool *pmp
	)
	:
	CXformGbAgg2HashAgg
		(
		 // pattern
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp),
							 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),
							 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)))
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAggDedup2HashAggDedup::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformGbAggDedup2HashAggDedup::Transform
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
	CLogicalGbAggDeduplicate *popAggDedup = CLogicalGbAggDeduplicate::PopConvert(pexpr->Pop());
	DrgPcr *pdrgpcr = popAggDedup->Pdrgpcr();
	pdrgpcr->AddRef();

	DrgPcr *pdrgpcrKeys = popAggDedup->PdrgpcrKeys();
	pdrgpcrKeys->AddRef();

	// extract components
	CExpression *pexprRel = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];
	GPOS_ASSERT(0 == pexprScalar->UlArity());

	// addref children
	pexprRel->AddRef();
	pexprScalar->AddRef();

	// create alternative expression
	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalHashAggDeduplicate
						(
						pmp,
						pdrgpcr,
						popAggDedup->PdrgpcrMinimal(),
						popAggDedup->Egbaggtype(),
						pdrgpcrKeys,
						popAggDedup->FGeneratesDuplicates(),
						CXformUtils::FMultiStageAgg(pexpr)
						),
			pexprRel,
			pexprScalar
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}

// EOF

