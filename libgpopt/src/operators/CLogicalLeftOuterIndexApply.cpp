//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//	Implementation of left outer index apply operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CLogicalLeftOuterIndexApply.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;

CLogicalLeftOuterIndexApply::CLogicalLeftOuterIndexApply
	(
	IMemoryPool *pmp
	)
	:
	CLogicalApply(pmp),
	m_pdrgpcrOuterRefs(NULL)
{
	m_fPattern = true;
}

CLogicalLeftOuterIndexApply::CLogicalLeftOuterIndexApply
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


CLogicalLeftOuterIndexApply::~CLogicalLeftOuterIndexApply()
{
	CRefCount::SafeRelease(m_pdrgpcrOuterRefs);
}


CMaxCard
CLogicalLeftOuterIndexApply::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}


CXformSet *
CLogicalLeftOuterIndexApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementLeftOuterIndexApply);

	return pxfs;
}

BOOL
CLogicalLeftOuterIndexApply::FMatch
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);

	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->FEqual(CLogicalLeftOuterIndexApply::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


COperator *
CLogicalLeftOuterIndexApply::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOuterRefs, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLeftOuterIndexApply(pmp, pdrgpcr);
}


IStatistics *
CLogicalLeftOuterIndexApply::PstatsDerive
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
	IStatistics *pstats = CStatisticsUtils::PstatsJoinArray(pmp, true /*fOuterJoin*/, pdrgpstat, pexprScalar);
	pdrgpstat->Release();

	return pstats;
}

// EOF

