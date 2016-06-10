//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CXformImplementPartitionSelector.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementPartitionSelector.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementPartitionSelector::CXformImplementPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementPartitionSelector::CXformImplementPartitionSelector
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
					GPOS_NEW(pmp) CLogicalPartitionSelector(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))	// relational child
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementPartitionSelector::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementPartitionSelector::Transform
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
	CLogicalPartitionSelector *popSelector = CLogicalPartitionSelector::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];

	IMDId *pmdid = popSelector->Pmdid();

	// addref all components
	pexprRelational->AddRef();
	pmdid->AddRef();

	HMUlExpr *phmulexprFilter = GPOS_NEW(pmp) HMUlExpr(pmp);

	const ULONG ulLevels = popSelector->UlPartLevels();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CExpression *pexprFilter = popSelector->PexprPartFilter(ul);
		GPOS_ASSERT(NULL != pexprFilter);
		pexprFilter->AddRef();
#ifdef GPOS_DEBUG
		BOOL fInserted =
#endif
		phmulexprFilter->FInsert(GPOS_NEW(pmp) ULONG(ul), pexprFilter);
		GPOS_ASSERT(fInserted);
	}

	// assemble physical operator
	CPhysicalPartitionSelectorDML *popPhysicalPartitionSelector =
			GPOS_NEW(pmp) CPhysicalPartitionSelectorDML
						(
						pmp,
						pmdid,
						phmulexprFilter,
						popSelector->PcrOid()
						);

	CExpression *pexprPartitionSelector =
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					popPhysicalPartitionSelector,
					pexprRelational
					);

	// add alternative to results
	pxfres->Add(pexprPartitionSelector);
}

// EOF

