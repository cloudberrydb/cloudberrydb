//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalLeftAntiSemiApplyNotIn.cpp
//
//	@doc:
//		Implementation of left anti-semi-apply operator with NotIn semantics
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftAntiSemiApplyNotIn.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiApplyNotIn::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftAntiSemiApplyNotIn::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotIn);
	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiApplyNotIn2LeftAntiSemiJoinNotInNoCorrelations);

	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiApplyNotIn::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftAntiSemiApplyNotIn::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftAntiSemiApplyNotIn(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

