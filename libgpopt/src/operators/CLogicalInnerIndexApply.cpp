//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CLogicalInnerIndexApply.cpp
//
//	@doc:
//		Implementation of index apply operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalInnerIndexApply.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::CLogicalInnerIndexApply
//
//	@doc:
//		Ctor - for patterns
//
//---------------------------------------------------------------------------
CLogicalInnerIndexApply::CLogicalInnerIndexApply
	(
	IMemoryPool *pmp
	)
	:
	CLogicalApply(pmp),
	m_pdrgpcrOuterRefs(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::CLogicalInnerIndexApply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalInnerIndexApply::CLogicalInnerIndexApply
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOuterRefs
	)
	:
	CLogicalApply(pmp),
	m_pdrgpcrOuterRefs(pdrgpcrOuterRefs)
{
	GPOS_ASSERT(NULL != pdrgpcrOuterRefs);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::~CLogicalInnerIndexApply
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalInnerIndexApply::~CLogicalInnerIndexApply()
{
	CRefCount::SafeRelease(m_pdrgpcrOuterRefs);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalInnerIndexApply::Maxcard
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
//		CLogicalInnerIndexApply::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInnerIndexApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementInnerIndexApply);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalInnerIndexApply::FMatch
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);

	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->FEqual(CLogicalInnerIndexApply::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalInnerIndexApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOuterRefs, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalInnerIndexApply(pmp, pdrgpcr);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalInnerIndexApply::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalInnerIndexApply::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat* // pdrgpstatCtxt
	)
	const
{
	GPOS_ASSERT(EspNone < Esp(exprhdl));

	IStatistics *pstatsOuter = exprhdl.Pstats(0);
	IStatistics *pstatsInner = exprhdl.Pstats(1);
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*ulChildIndex*/);

	// join stats of the children
	DrgPstat *pdrgpstat = GPOS_NEW(pmp) DrgPstat(pmp);
	pstatsOuter->AddRef();
	pdrgpstat->Append(pstatsOuter);
	pstatsInner->AddRef();
	pdrgpstat->Append(pstatsInner);
	IStatistics *pstats = CStatisticsUtils::PstatsJoinArray(pmp, false /*fOuterJoin*/, pdrgpstat, pexprScalar);
	pdrgpstat->Release();

	return pstats;
}

// EOF

