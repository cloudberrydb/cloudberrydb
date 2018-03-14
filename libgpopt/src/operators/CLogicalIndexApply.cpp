//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	Implementation of inner / left outer index apply operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CLogicalIndexApply.h"
#include "naucrates/statistics/CJoinStatsProcessor.h"

using namespace gpopt;

CLogicalIndexApply::CLogicalIndexApply
	(
	IMemoryPool *pmp
	)
	:
	CLogicalApply(pmp),
	m_pdrgpcrOuterRefs(NULL),
	m_fOuterJoin(false)
{
	m_fPattern = true;
}

CLogicalIndexApply::CLogicalIndexApply
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOuterRefs,
	BOOL fOuterJoin
	)
	:
	CLogicalApply(pmp),
	m_pdrgpcrOuterRefs(pdrgpcrOuterRefs),
	m_fOuterJoin(fOuterJoin)
{
	GPOS_ASSERT(NULL != pdrgpcrOuterRefs);
}


CLogicalIndexApply::~CLogicalIndexApply()
{
	CRefCount::SafeRelease(m_pdrgpcrOuterRefs);
}


CMaxCard
CLogicalIndexApply::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}


CXformSet *
CLogicalIndexApply::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementIndexApply);
	return pxfs;
}

BOOL
CLogicalIndexApply::FMatch
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);

	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->FEqual(CLogicalIndexApply::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


IStatistics *
CLogicalIndexApply::PstatsDerive
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
	IStatistics::EStatsJoinType eStatsJoinType = IStatistics::EsjtInnerJoin;
	// we use Inner Join semantics here except in the case of Left Outer Join
	if (m_fOuterJoin)
	{
		eStatsJoinType = IStatistics::EsjtLeftOuterJoin;
	}
	IStatistics *pstats = CJoinStatsProcessor::PstatsJoinArray(pmp, pdrgpstat, pexprScalar, eStatsJoinType);
	pdrgpstat->Release();

	return pstats;
}

// return a copy of the operator with remapped columns
COperator *
CLogicalIndexApply::PopCopyWithRemappedColumns
(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOuterRefs, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalIndexApply(pmp, pdrgpcr, m_fOuterJoin);
}

// EOF

