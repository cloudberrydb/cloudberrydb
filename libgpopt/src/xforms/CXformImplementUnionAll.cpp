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
#include "gpopt/operators/CPhysicalSerialUnionAll.h"

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
	DrgPcr *pdrgpcrOutput = popUnionAll->PdrgpcrOutput();
	DrgDrgPcr *pdrgpdrgpcrInput = popUnionAll->PdrgpdrgpcrInput();

	// TODO:  May 2nd 2012; support compatible types
	if (!CXformUtils::FSameDatatype(pdrgpdrgpcrInput))
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, GPOS_WSZ_LIT("Union of non-identical types"));
	}

	if (GPOS_FTRACE(EopttraceEnableParallelAppend))
	{
		GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsupportedOp, GPOS_WSZ_LIT("Parallel Append is not supported yet"));
	}

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

	// assemble physical operator
	CExpression *pexprUnionAll =
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CPhysicalSerialUnionAll
						(
						pmp,
						pdrgpcrOutput,
						pdrgpdrgpcrInput,
						popUnionAll->UlScanIdPartialIndex()
						),
					pdrgpexpr
					);

	// add alternative to results
	pxfres->Add(pexprUnionAll);
}

// EOF

