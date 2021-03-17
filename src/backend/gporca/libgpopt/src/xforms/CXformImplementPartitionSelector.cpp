//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CXformImplementPartitionSelector.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpopt/xforms/CXformImplementPartitionSelector.h"

#include "gpos/base.h"

#include "gpopt/operators/CLogicalPartitionSelector.h"
#include "gpopt/operators/CPatternLeaf.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementPartitionSelector::CXformImplementPartitionSelector
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementPartitionSelector::CXformImplementPartitionSelector(
	CMemoryPool *mp)
	:  // pattern
	  CXformImplementation(GPOS_NEW(mp) CExpression(
		  mp, GPOS_NEW(mp) CLogicalPartitionSelector(mp),
		  GPOS_NEW(mp) CExpression(
			  mp, GPOS_NEW(mp) CPatternLeaf(mp))  // relational child
		  ))
{
}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementPartitionSelector::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementPartitionSelector::Transform(CXformContext *pxfctxt,
											CXformResult *pxfres GPOS_UNUSED,
											CExpression *pexpr) const
{
	GPOS_ASSERT(nullptr != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	CMemoryPool *mp = pxfctxt->Pmp();

	// extract components
	CLogicalPartitionSelector *popSelector =
		CLogicalPartitionSelector::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];

	IMDId *mdid = popSelector->MDId();

	// addref all components
	pexprRelational->AddRef();
	mdid->AddRef();

	UlongToExprMap *phmulexprFilter = GPOS_NEW(mp) UlongToExprMap(mp);

	const ULONG ulLevels = popSelector->UlPartLevels();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CExpression *pexprFilter = popSelector->PexprPartFilter(ul);
		GPOS_ASSERT(nullptr != pexprFilter);
		pexprFilter->AddRef();
		BOOL fInserted GPOS_ASSERTS_ONLY =
			phmulexprFilter->Insert(GPOS_NEW(mp) ULONG(ul), pexprFilter);
		GPOS_ASSERT(fInserted);
	}
}

// EOF
