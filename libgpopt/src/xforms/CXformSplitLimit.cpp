//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSplitLimit.cpp
//
//	@doc:
//		Implementation of the splitting of limit
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
#include "gpopt/xforms/CXformSplitLimit.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::CXformSplitLimit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSplitLimit::CXformSplitLimit
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
					GPOS_NEW(pmp) CLogicalLimit(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),  // scalar child for offset
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))  // scalar child for number of rows
					)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSplitLimit::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (0 < exprhdl.Pdprel()->PcrsOuter()->CElements())
	{
		return CXform::ExfpNone;
	}

	CLogicalLimit *popLimit = CLogicalLimit::PopConvert(exprhdl.Pop());
	if (!popLimit->FGlobal() || !popLimit->FHasCount())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::Transform
//
//	@doc:
//		Actual transformation to expand a global limit into a pair of
//		local and global limit
//
//---------------------------------------------------------------------------
void
CXformSplitLimit::Transform
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
	CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalarStart = (*pexpr)[1];
	CExpression *pexprScalarRows = (*pexpr)[2];
	COrderSpec *pos = popLimit->Pos();

	// get relational properties
	CDrvdPropRelational *pdprel =
			CDrvdPropRelational::Pdprel(pexprRelational->Pdp(CDrvdProp::EptRelational));

	// TODO: , Feb 20, 2012, we currently only split limit with offset 0.
	if (!CUtils::FHasZeroOffset(pexpr) || 0 < pdprel->PcrsOuter()->CElements())
	{
		return;
	}

	// addref all components
	pexprRelational->AddRef();

	// assemble local limit operator
	CExpression *pexprLimitLocal = PexprLimit
			(
			pmp,
			pexprRelational,
			pexprScalarStart,
			pexprScalarRows,
			pos,
			false, // fGlobal
			popLimit->FHasCount(),
			popLimit->FTopLimitUnderDML()
			);

	// assemble global limit operator
	CExpression *pexprLimitGlobal = PexprLimit
			(
			pmp,
			pexprLimitLocal,
			pexprScalarStart,
			pexprScalarRows,
			pos,
			true, // fGlobal
			popLimit->FHasCount(),
			popLimit->FTopLimitUnderDML()
			);

	pxfres->Add(pexprLimitGlobal);
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSplitLimit::PexprLimit
//
//	@doc:
//		Generate a limit operator
//
//---------------------------------------------------------------------------
CExpression *
CXformSplitLimit::PexprLimit
	(
	IMemoryPool *pmp,
	CExpression *pexprRelational,
	CExpression *pexprScalarStart,
	CExpression *pexprScalarRows,
	COrderSpec *pos,
	BOOL fGlobal,
	BOOL fHasCount,
	BOOL fTopLimitUnderDML
	)
	const
{
	pexprScalarStart->AddRef();
	pexprScalarRows->AddRef();
	pos->AddRef();

	// assemble global limit operator
	CExpression *pexprLimit = GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalLimit(pmp, pos, fGlobal, fHasCount, fTopLimitUnderDML),
			pexprRelational,
			pexprScalarStart,
			pexprScalarRows
			);

	return pexprLimit;
}

// EOF
