//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CXformSplitGbAggDedup.cpp
//
//	@doc:
//		Implementation of the splitting of a dedup aggregate into a pair of
//		local and global aggregates
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
#include "gpopt/base/CColRefComputed.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformSplitGbAggDedup.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "naucrates/md/IMDAggregate.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAggDedup::CXformSplitGbAggDedup
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitGbAggDedup::CXformSplitGbAggDedup
	(
	IMemoryPool *pmp
	)
	:
	CXformSplitGbAgg
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitGbAggDedup::Transform
//
//	@doc:
//		Actual transformation to expand a global aggregate into a pair of
//		local and global aggregate
//
//---------------------------------------------------------------------------
void
CXformSplitGbAggDedup::Transform
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
	CLogicalGbAggDeduplicate *popAggDedup = CLogicalGbAggDeduplicate::PopConvert(pexpr->Pop());

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	// check if the transformation is applicable
	if (!FApplicable(pexprProjectList))
	{
		return;
	}

	pexprRelational->AddRef();

	CExpression *pexprProjectListLocal = NULL;
	CExpression *pexprProjectListGlobal = NULL;

	(void) PopulateLocalGlobalProjectList(pmp, pexprProjectList, &pexprProjectListLocal, &pexprProjectListGlobal);
	GPOS_ASSERT(NULL != pexprProjectListLocal && NULL != pexprProjectListLocal);

	DrgPcr *pdrgpcr = popAggDedup->Pdrgpcr();
	pdrgpcr->AddRef();
	pdrgpcr->AddRef();

	DrgPcr *pdrgpcrMinimal = popAggDedup->PdrgpcrMinimal();
	if (NULL != pdrgpcrMinimal)
	{
		pdrgpcrMinimal->AddRef();
		pdrgpcrMinimal->AddRef();
	}

	DrgPcr *pdrgpcrKeys = popAggDedup->PdrgpcrKeys();
	pdrgpcrKeys->AddRef();
	pdrgpcrKeys->AddRef();

	CExpression *pexprLocal =
			GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp, pdrgpcr, pdrgpcrMinimal, COperator::EgbaggtypeLocal /*egbaggtype*/, pdrgpcrKeys),
						pexprRelational,
						pexprProjectListLocal
						);

	CExpression *pexprGlobal =
			GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp, pdrgpcr, pdrgpcrMinimal, COperator::EgbaggtypeGlobal /*egbaggtype*/, pdrgpcrKeys),
						pexprLocal,
						pexprProjectListGlobal
						);

	pxfres->Add(pexprGlobal);
}

// EOF
