//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInnerCorrelatedApply.cpp
//
//	@doc:
//		Implementation of inner correlated apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalInnerCorrelatedApply.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
//
//	@doc:
//		Ctor - for patterns
//
//---------------------------------------------------------------------------
CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
	(
	IMemoryPool *pmp
	)
	:
	CLogicalInnerApply(pmp)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalInnerCorrelatedApply::CLogicalInnerCorrelatedApply
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrInner,
	EOperatorId eopidOriginSubq
	)
	:
	CLogicalInnerApply(pmp, pdrgpcrInner, eopidOriginSubq)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInnerCorrelatedApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementInnerCorrelatedApply);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalInnerCorrelatedApply::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrInner->FEqual(CLogicalInnerCorrelatedApply::PopConvert(pop)->PdrgPcrInner());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerCorrelatedApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalInnerCorrelatedApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrInner = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInner, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalInnerCorrelatedApply(pmp, pdrgpcrInner, m_eopidOriginSubq);
}

// EOF

