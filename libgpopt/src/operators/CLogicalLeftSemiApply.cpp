//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftSemiApply.cpp
//
//	@doc:
//		Implementation of left semi-apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/operators/CLogicalLeftSemiApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftSemiApply::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, exprhdl.Pdprel(0)->Maxcard());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftSemiApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiApply2LeftSemiJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiApplyWithExternalCorrs2InnerJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiApply2LeftSemiJoinNoCorrelations);

	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLeftSemiApply::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(3 == exprhdl.UlArity());

	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftSemiApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftSemiApply(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

