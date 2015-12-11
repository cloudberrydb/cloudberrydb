//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalLeftOuterApply.cpp
//
//	@doc:
//		Implementation of left outer apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftOuterApply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::CLogicalLeftOuterApply
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterApply::CLogicalLeftOuterApply
	(
	IMemoryPool *pmp
	)
	:
	CLogicalApply(pmp)
{
	GPOS_ASSERT(NULL != pmp);

	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::CLogicalLeftOuterApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterApply::CLogicalLeftOuterApply
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrInner,
	EOperatorId eopidOriginSubq
	)
	:
	CLogicalApply(pmp, pdrgpcrInner, eopidOriginSubq)
{
	GPOS_ASSERT(0 < pdrgpcrInner->UlLength());
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::~CLogicalLeftOuterApply
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterApply::~CLogicalLeftOuterApply()
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftOuterApply::Maxcard
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
//		CLogicalLeftOuterApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftOuterApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfLeftOuterApply2LeftOuterJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftOuterApply2LeftOuterJoinNoCorrelations);

	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftOuterApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftOuterApply(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

