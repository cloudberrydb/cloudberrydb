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
	IMemoryPool *mp
	)
	:
	CLogicalApply(mp),
	m_pdrgpcrOuterRefs(NULL),
	m_fOuterJoin(false)
{
	m_fPattern = true;
}

CLogicalIndexApply::CLogicalIndexApply
	(
	IMemoryPool *mp,
	CColRefArray *pdrgpcrOuterRefs,
	BOOL fOuterJoin
	)
	:
	CLogicalApply(mp),
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
	IMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}


CXformSet *
CLogicalIndexApply::PxfsCandidates
	(
	IMemoryPool *mp
	)
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfImplementIndexApply);
	return xform_set;
}

BOOL
CLogicalIndexApply::Matches
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);

	if (pop->Eopid() == Eopid())
	{
		return m_pdrgpcrOuterRefs->Equals(CLogicalIndexApply::PopConvert(pop)->PdrgPcrOuterRefs());
	}

	return false;
}


IStatistics *
CLogicalIndexApply::PstatsDerive
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	IStatisticsArray* // stats_ctxt
	)
	const
{
	GPOS_ASSERT(EspNone < Esp(exprhdl));

	IStatistics *outer_stats = exprhdl.Pstats(0);
	IStatistics *inner_side_stats = exprhdl.Pstats(1);
	CExpression *pexprScalar = exprhdl.PexprScalarChild(2 /*child_index*/);

	// join stats of the children
	IStatisticsArray *statistics_array = GPOS_NEW(mp) IStatisticsArray(mp);
	outer_stats->AddRef();
	statistics_array->Append(outer_stats);
	inner_side_stats->AddRef();
	statistics_array->Append(inner_side_stats);
	IStatistics::EStatsJoinType join_type = IStatistics::EsjtInnerJoin;
	// we use Inner Join semantics here except in the case of Left Outer Join
	if (m_fOuterJoin)
	{
		join_type = IStatistics::EsjtLeftOuterJoin;
	}
	IStatistics *stats = CJoinStatsProcessor::CalcAllJoinStats(mp, statistics_array, pexprScalar, join_type);
	statistics_array->Release();

	return stats;
}

// return a copy of the operator with remapped columns
COperator *
CLogicalIndexApply::PopCopyWithRemappedColumns
(
	IMemoryPool *mp,
	UlongToColRefMap *colref_mapping,
	BOOL must_exist
	)
{
	CColRefArray *colref_array = CUtils::PdrgpcrRemap(mp, m_pdrgpcrOuterRefs, colref_mapping, must_exist);

	return GPOS_NEW(mp) CLogicalIndexApply(mp, colref_array, m_fOuterJoin);
}

// EOF

