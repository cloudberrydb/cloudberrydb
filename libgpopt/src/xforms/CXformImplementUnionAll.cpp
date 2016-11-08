//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementUnionAll.cpp
//
//	@doc:
//		Implementation of union all operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/exception.h"
#include "gpopt/xforms/CXformImplementUnionAll.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/operators/CPhysicalUnionAll.h"
#include "gpopt/operators/CPhysicalUnionAllFactory.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformImplementUnionAll::CXformImplementUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementUnionAll::CXformImplementUnionAll
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
						GPOS_NEW(pmp) CLogicalUnionAll(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiLeaf(pmp))
						)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementUnionAll::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformImplementUnionAll::Transform
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
	CLogicalUnionAll *popUnionAll = CLogicalUnionAll::PopConvert(pexpr->Pop());
	CPhysicalUnionAllFactory factory(popUnionAll);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexpr->UlArity();

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprChild = (*pexpr)[ul];
		pexprChild->AddRef();
		pdrgpexpr->Append(pexprChild);
	}

	CPhysicalUnionAll *popPhysicalSerialUnionAll = factory.PopPhysicalUnionAll(pmp, false);

	// assemble serial union physical operator
	CExpression *pexprSerialUnionAll =
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					popPhysicalSerialUnionAll,
					pdrgpexpr
					);

	// add serial union alternative to results
	pxfres->Add(pexprSerialUnionAll);

	// parallel union alternative to the result if the GUC is on
	BOOL fParallel = GPOS_FTRACE(EopttraceEnableParallelAppend);

	if(fParallel)
	{
		CPhysicalUnionAll *popPhysicalParallelUnionAll = factory.PopPhysicalUnionAll(pmp, true);

		pdrgpexpr->AddRef();

		// assemble physical parallel operator
		CExpression *pexprParallelUnionAll =
		GPOS_NEW(pmp) CExpression
		(
		 pmp,
		 popPhysicalParallelUnionAll,
		 pdrgpexpr
		 );

		// add parallel union alternative to results
		pxfres->Add(pexprParallelUnionAll);
	}
}

// EOF

