//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatsPredPoint.cpp
//
//	@doc:
//		Implementation of statistics filter that compares a column to a constant
//---------------------------------------------------------------------------

#include "naucrates/statistics/CStatsPredPoint.h"
#include "naucrates/md/CMDIdGPDB.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CColRefTable.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredPoint::CStatisticsFilterPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredPoint::CStatsPredPoint
	(
	ULONG ulColId,
	CStatsPred::EStatsCmpType escmpt,
	CPoint *ppoint
	)
	:
	CStatsPred(ulColId),
	m_escmpt(escmpt),
	m_ppoint(ppoint)
{
	GPOS_ASSERT(NULL != ppoint);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredPoint::CStatisticsFilterPoint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CStatsPredPoint::CStatsPredPoint
	(
	IMemoryPool *pmp,
	const CColRef *pcr,
	CStatsPred::EStatsCmpType escmpt,
	IDatum *pdatum
	)
	:
	CStatsPred(gpos::ulong_max),
	m_escmpt(escmpt),
	m_ppoint(NULL)
{
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(NULL != pdatum);

	m_ulColId = pcr->UlId();
	IDatum *pdatumPadded = PdatumPreprocess(pmp, pcr, pdatum);

	m_ppoint = GPOS_NEW(pmp) CPoint(pdatumPadded);
}

//---------------------------------------------------------------------------
//		CStatsPredPoint::PdatumPreprocess
//
//	@doc:
//		Add padding to datums when needed
//---------------------------------------------------------------------------
IDatum *
CStatsPredPoint::PdatumPreprocess
	(
	IMemoryPool *pmp,
	const CColRef *pcr,
	IDatum *pdatum
	)
{
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(NULL != pdatum);

	if (!pdatum->FNeedsPadding() || CColRef::EcrtTable != pcr->Ecrt() || pdatum->FNull())
	{
		// we do not pad datum for comparison against computed columns
		pdatum->AddRef();
		return pdatum;
	}

	const CColRefTable *pcrTable = CColRefTable::PcrConvert(const_cast<CColRef*>(pcr));

	return pdatum->PdatumPadded(pmp, pcrTable->UlWidth());
}

// EOF

