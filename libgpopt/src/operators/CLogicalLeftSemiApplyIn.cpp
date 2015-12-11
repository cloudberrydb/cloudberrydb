//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CLogicalLeftSemiApplyIn.cpp
//
//	@doc:
//		Implementation of left-semi-apply operator with In semantics
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftSemiApplyIn.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApplyIn::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftSemiApplyIn::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiApplyIn2LeftSemiJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiApplyInWithExternalCorrs2InnerJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiApplyIn2LeftSemiJoinNoCorrelations);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiApplyIn::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftSemiApplyIn::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftSemiApplyIn(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

