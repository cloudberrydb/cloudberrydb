//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CPhysicalStreamAggDeduplicate.cpp
//
//	@doc:
//		Implementation of stream aggregation operator for deduplicating join outputs
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalStreamAggDeduplicate.h"
#include "gpopt/operators/CLogicalGbAgg.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAggDeduplicate::CPhysicalStreamAggDeduplicate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalStreamAggDeduplicate::CPhysicalStreamAggDeduplicate
	(
	IMemoryPool *mp,
	CColRefArray *colref_array,
	CColRefArray *pdrgpcrMinimal,
	COperator::EGbAggType egbaggtype,
	CColRefArray *pdrgpcrKeys,
	BOOL fGeneratesDuplicates,
	BOOL fMultiStage
	)
	:
	CPhysicalStreamAgg(mp, colref_array, pdrgpcrMinimal, egbaggtype, fGeneratesDuplicates, NULL /*pdrgpcrGbMinusDistinct*/, fMultiStage),
	m_pdrgpcrKeys(pdrgpcrKeys)
{
	GPOS_ASSERT(NULL != pdrgpcrKeys);
	InitOrderSpec(mp, m_pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAggDeduplicate::~CPhysicalStreamAggDeduplicate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalStreamAggDeduplicate::~CPhysicalStreamAggDeduplicate()
{
	m_pdrgpcrKeys->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAggDeduplicate::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalStreamAggDeduplicate::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os	<< SzId()
		<< "( ";
	CLogicalGbAgg::OsPrintGbAggType(os, Egbaggtype());
	os	<< " )"
		<< " Grp Cols: [";

	CUtils::OsPrintDrgPcr(os, PdrgpcrGroupingCols());
	os	<< "]"
		<< ", Key Cols:[";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrKeys);
	os	<< "]";

	os	<< ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";

	return os;
}

// EOF

