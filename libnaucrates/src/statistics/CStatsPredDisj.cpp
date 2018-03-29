//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredDisj.cpp
//
//	@doc:
//		Implementation of statistics Disjunctive filter
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::CStatsPrefDisj
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredDisj::CStatsPredDisj
	(
	DrgPstatspred *pdrgpstatspred
	)
	:
	CStatsPred(gpos::ulong_max),
	m_pdrgpstatspred(pdrgpstatspred)
{
	GPOS_ASSERT(NULL != pdrgpstatspred);
	m_ulColId = CStatisticsUtils::UlColId(pdrgpstatspred);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::Pstatspred
//
//	@doc:
//		Return the point filter at a particular position
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredDisj::Pstatspred
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpstatspred)[ulPos];
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::Sort
//
//	@doc:
//		Sort the components of the disjunction
//
//---------------------------------------------------------------------------
void
CStatsPredDisj::Sort() const
{
	if (1 < UlFilters())
	{
		// sort the filters on column ids
		m_pdrgpstatspred->Sort(CStatsPred::IStatsFilterCmp);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPrefDisj::UlColId
//
//	@doc:
//		Return the column identifier on which the predicates are on
//
//---------------------------------------------------------------------------
ULONG
CStatsPredDisj::UlColId() const
{
	return m_ulColId;
}

// EOF
