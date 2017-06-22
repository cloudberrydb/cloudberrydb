//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CPhysicalDynamicScan.cpp
//
//	@doc:
//		Base class for physical dynamic scan operators
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpopt/operators/CPhysicalDynamicScan.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDrvdPropCtxtPlan.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CPartConstraint.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;
using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::CPhysicalDynamicScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalDynamicScan::CPhysicalDynamicScan
	(
	IMemoryPool *pmp,
	BOOL fPartial,
	CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId,
	const CName *pnameAlias,
	ULONG ulScanId,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrParts,
	ULONG ulSecondaryScanId,
	CPartConstraint *ppartcnstr,
	CPartConstraint *ppartcnstrRel
	)
	:
	CPhysicalScan(pmp, pnameAlias, ptabdesc, pdrgpcrOutput),
	m_ulOriginOpId(ulOriginOpId),
	m_fPartial(fPartial),
	m_ulScanId(ulScanId),
	m_pdrgpdrgpcrPart(pdrgpdrgpcrParts),
	m_ulSecondaryScanId(ulSecondaryScanId),
	m_ppartcnstr(ppartcnstr),
	m_ppartcnstrRel(ppartcnstrRel)
{
	GPOS_ASSERT(NULL != pdrgpdrgpcrParts);
	GPOS_ASSERT(0 < pdrgpdrgpcrParts->UlLength());
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(NULL != ppartcnstrRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::~CPhysicalDynamicScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalDynamicScan::~CPhysicalDynamicScan()
{
	m_pdrgpdrgpcrPart->Release();
	m_ppartcnstr->Release();
	m_ppartcnstrRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::UlHash
//
//	@doc:
//		Combine part index, pointer for table descriptor, Eop and output columns
//
//---------------------------------------------------------------------------
ULONG
CPhysicalDynamicScan::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(),
								gpos::UlCombineHashes(gpos::UlHash(&m_ulScanId),
								                      m_ptabdesc->Pmdid()->UlHash()));
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrOutput));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::PpimDerive
//
//	@doc:
//		Derive partition index map
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalDynamicScan::PpimDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &, //exprhdl
	CDrvdPropCtxt *pdpctxt
	)
	const
{
	GPOS_ASSERT(NULL != pdpctxt);
	IMDId *pmdid = m_ptabdesc->Pmdid();
	pmdid->AddRef();
	m_pdrgpdrgpcrPart->AddRef();
	m_ppartcnstr->AddRef();
	m_ppartcnstrRel->AddRef();
	ULONG ulExpectedPartitionSelectors = CDrvdPropCtxtPlan::PdpctxtplanConvert(pdpctxt)->UlExpectedPartitionSelectors();

	return PpimDeriveFromDynamicScan(pmp, m_ulScanId, pmdid, m_pdrgpdrgpcrPart, m_ulSecondaryScanId, m_ppartcnstr, m_ppartcnstrRel, ulExpectedPartitionSelectors);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalDynamicScan::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " ";

	// alias of table as referenced in the query
	m_pnameAlias->OsPrint(os);

	// actual name of table in catalog and columns
	os << " (";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "] Scan Id: " << m_ulScanId << "." << m_ulSecondaryScanId;

	if (!m_ppartcnstr->FUnbounded())
	{
		os << ", ";
		m_ppartcnstr->OsPrint(os);
	}

	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDynamicScan::PopConvert
//
//	@doc:
//		conversion function
//
//---------------------------------------------------------------------------
CPhysicalDynamicScan *
CPhysicalDynamicScan::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(CUtils::FPhysicalScan(pop) && CPhysicalScan::PopConvert(pop)->FDynamicScan());

	return dynamic_cast<CPhysicalDynamicScan *>(pop);
}


// EOF
