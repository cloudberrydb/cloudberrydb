//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalInnerApply.cpp
//
//	@doc:
//		Implementation of inner apply operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalInnerApply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerApply::CLogicalInnerApply
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalInnerApply::CLogicalInnerApply
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
//		CLogicalInnerApply::CLogicalInnerApply
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalInnerApply::CLogicalInnerApply
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
//		CLogicalInnerApply::~CLogicalInnerApply
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalInnerApply::~CLogicalInnerApply()
{}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerApply::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalInnerApply::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInnerApply::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	
	(void) pxfs->FExchangeSet(CXform::ExfInnerApply2InnerJoin);
	(void) pxfs->FExchangeSet(CXform::ExfInnerApply2InnerJoinNoCorrelations);
	(void) pxfs->FExchangeSet(CXform::ExfInnerApplyWithOuterKey2InnerJoin);
	
	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalInnerApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalInnerApply(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

