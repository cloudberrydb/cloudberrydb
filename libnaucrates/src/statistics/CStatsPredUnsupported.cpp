//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CStatsPredUnsupported.cpp
//
//	@doc:
//		Implementation of unsupported statistics predicate
//---------------------------------------------------------------------------

#include "naucrates/statistics/CHistogram.h"
#include "naucrates/statistics/CStatsPredUnsupported.h"
#include "naucrates/statistics/CStatistics.h"

using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUnsupported::CStatsPredUnsupported
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredUnsupported::CStatsPredUnsupported
	(
	ULONG ulColId,
	CStatsPred::EStatsCmpType estatscmptype
	)
	:
	CStatsPred(ulColId),
	m_estatscmptype(estatscmptype),
	m_dDefaultScaleFactor(0.0)
{
	m_dDefaultScaleFactor = DScaleFactorInit();
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUnsupported::CStatsPredUnsupported
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredUnsupported::CStatsPredUnsupported
	(
	ULONG ulColId,
	CStatsPred::EStatsCmpType estatscmptype,
	CDouble dDefaultScaleFactor
	)
	:
	CStatsPred(ulColId),
	m_estatscmptype(estatscmptype),
	m_dDefaultScaleFactor(dDefaultScaleFactor)
{
	GPOS_ASSERT(CStatistics::DEpsilon < dDefaultScaleFactor);
}


//---------------------------------------------------------------------------
//		CStatsPredUnsupported::DScaleFactorInit
//
//	@doc:
//		Initialize the scale factor of the unknown predicate
//---------------------------------------------------------------------------
CDouble
CStatsPredUnsupported::DScaleFactorInit()
{
	return (1 / CHistogram::DDefaultSelectivity).DVal();
}

// EOF
