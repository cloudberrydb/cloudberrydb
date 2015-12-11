//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftAntiSemiApply.cpp
//
//	@doc:
//		Implementation of left anti-semi-apply operator
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

#include "gpopt/operators/CLogicalLeftAntiSemiApply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiApply::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalLeftAntiSemiApply::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiApply::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftAntiSemiApply::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftAntiSemiApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiApply2LeftAntiSemiJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiApply2LeftAntiSemiJoinNoCorrelations);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftAntiSemiApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftAntiSemiApply(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

