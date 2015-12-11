//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalLeftOuterCorrelatedApply.cpp
//
//	@doc:
//		Implementation of left outer correlated apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftOuterCorrelatedApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply
//
//	@doc:
//		Ctor - for patterns
//
//---------------------------------------------------------------------------
CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply
	(
	IMemoryPool *pmp
	)
	:
	CLogicalLeftOuterApply(pmp)
{}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterCorrelatedApply::CLogicalLeftOuterCorrelatedApply
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrInner,
	EOperatorId eopidOriginSubq
	)
	:
	CLogicalLeftOuterApply(pmp, pdrgpcrInner, eopidOriginSubq)
{}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftOuterCorrelatedApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementLeftOuterCorrelatedApply);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalLeftOuterCorrelatedApply::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrInner->FEqual(CLogicalLeftOuterCorrelatedApply::PopConvert(pop)->PdrgPcrInner());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterCorrelatedApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLeftOuterCorrelatedApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftOuterCorrelatedApply(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

