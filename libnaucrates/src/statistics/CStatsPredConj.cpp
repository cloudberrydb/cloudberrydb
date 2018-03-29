//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredConj.cpp
//
//	@doc:
//		Implementation of statistics Conjunctive filter
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredConj.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpnaucrates;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::CStatsPredConj
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredConj::CStatsPredConj
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
//		CStatsPredConj::Pstatspred
//
//	@doc:
//		Return the filter at a particular position
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredConj::Pstatspred
	(
	ULONG ulPos
	)
	const
{
	return (*m_pdrgpstatspred)[ulPos];
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::Sort
//
//	@doc:
//		Sort the components of the disjunction
//
//---------------------------------------------------------------------------
void
CStatsPredConj::Sort() const
{
	if (1 < UlFilters())
	{
		// sort the filters on column ids
		m_pdrgpstatspred->Sort(CStatsPred::IStatsFilterCmp);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredConj::UlColId
//
//	@doc:
//		Return the column identifier on which the predicates are on
//
//---------------------------------------------------------------------------
ULONG
CStatsPredConj::UlColId() const
{
	return m_ulColId;
}

// EOF
