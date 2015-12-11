//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CPhysicalBitmapTableScan.cpp
//
//	@doc:
//		Bitmap table scan physical operator
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalBitmapTableScan.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalBitmapTableScan::CPhysicalBitmapTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalBitmapTableScan::CPhysicalBitmapTableScan
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId,
	const CName *pnameTableAlias,
	DrgPcr *pdrgpcrOutput
	)
	:
	CPhysicalScan(pmp, pnameTableAlias, ptabdesc, pdrgpcrOutput),
	m_ulOriginOpId(ulOriginOpId)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalBitmapTableScan::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalBitmapTableScan::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalBitmapTableScan::UlHash
//
//	@doc:
//		Match this operator with the given one.
//
//---------------------------------------------------------------------------
BOOL
CPhysicalBitmapTableScan::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchBitmapScan(this, pop);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalBitmapTableScan::OsPrint
//
//	@doc:
//		Debug print of a CPhysicalBitmapTableScan
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalBitmapTableScan::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " ";
	os << ", Table Name: (";
	m_ptabdesc->Name().OsPrint(os);
	os <<")";
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "]";

	return os;
}

// EOF
