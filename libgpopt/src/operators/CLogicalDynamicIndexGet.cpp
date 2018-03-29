//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDynamicIndexGet.cpp
//
//	@doc:
//		Implementation of index access for partitioned tables
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalDynamicIndexGet.h"
#include "gpopt/metadata/CName.h"
#include "gpopt/metadata/CPartConstraint.h"

#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::CLogicalDynamicIndexGet
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDynamicIndexGet::CLogicalDynamicIndexGet
	(
	IMemoryPool *pmp
	)
	:
	CLogicalDynamicGetBase(pmp),
	m_pindexdesc(NULL),
	m_ulOriginOpId(gpos::ulong_max),
	m_pos(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::CLogicalDynamicIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalDynamicIndexGet::CLogicalDynamicIndexGet
	(
	IMemoryPool *pmp,
	const IMDIndex *pmdindex,
	CTableDescriptor *ptabdesc,
	ULONG ulOriginOpId,
	const CName *pnameAlias,
	ULONG ulPartIndexId,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrPart,
	ULONG ulSecondaryPartIndexId,
	CPartConstraint *ppartcnstr,
	CPartConstraint *ppartcnstrRel
	)
	:
	CLogicalDynamicGetBase(pmp, pnameAlias, ptabdesc, ulPartIndexId, pdrgpcrOutput, pdrgpdrgpcrPart, ulSecondaryPartIndexId, FPartialIndex(ptabdesc, pmdindex), ppartcnstr, ppartcnstrRel),
	m_pindexdesc(NULL),
	m_ulOriginOpId(ulOriginOpId)
{
	GPOS_ASSERT(NULL != pmdindex);

	// create the index descriptor
	m_pindexdesc  = CIndexDescriptor::Pindexdesc(pmp, ptabdesc, pmdindex);

	// compute the order spec
	m_pos = PosFromIndex(m_pmp, pmdindex, m_pdrgpcrOutput, ptabdesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::~CLogicalDynamicIndexGet
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalDynamicIndexGet::~CLogicalDynamicIndexGet()
{
	CRefCount::SafeRelease(m_pindexdesc);
	CRefCount::SafeRelease(m_pos);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalDynamicIndexGet::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(),
	                             gpos::UlCombineHashes(gpos::UlHash(&m_ulScanId),
					             m_pindexdesc->Pmdid()->UlHash()));
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalDynamicIndexGet::FMatch
	(
	COperator *pop
	)
	const
{
	return CUtils::FMatchDynamicIndex(this, pop);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::PcrsDeriveOuter
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalDynamicIndexGet::PcrsDeriveOuter
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOuterIndexGet(pmp, exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalDynamicIndexGet::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDIndex *pmdindex = pmda->Pmdindex(m_pindexdesc->Pmdid());
	CName *pnameAlias = GPOS_NEW(pmp) CName(pmp, *m_pnameAlias);

	DrgPcr *pdrgpcrOutput = NULL;
	if (fMustExist)
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemapAndCreate(pmp, m_pdrgpcrOutput, phmulcr);
	}
	else
	{
		pdrgpcrOutput = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOutput, phmulcr, fMustExist);
	}

	DrgDrgPcr *pdrgpdrgpcrPart = CUtils::PdrgpdrgpcrRemap(pmp, m_pdrgpdrgpcrPart, phmulcr, fMustExist);
	CPartConstraint *ppartcnstr = m_ppartcnstr->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
	CPartConstraint *ppartcnstrRel = m_ppartcnstrRel->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);

	m_ptabdesc->AddRef();

	return GPOS_NEW(pmp) CLogicalDynamicIndexGet
						(
						pmp,
						pmdindex,
						m_ptabdesc,
						m_ulOriginOpId,
						pnameAlias,
						m_ulScanId,
						pdrgpcrOutput,
						pdrgpdrgpcrPart,
						m_ulSecondaryScanId,
						ppartcnstr,
						ppartcnstrRel
						);
}

// Checking if index is partial given the table descriptor and mdid of the index
BOOL
CLogicalDynamicIndexGet::FPartialIndex
	(
	CTableDescriptor *ptabdesc,
	const IMDIndex *pmdindex
	)
{
	// refer to the relation on which this index is defined for index partial information
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());
	return pmdrel->FPartialIndex(pmdindex->Pmdid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::FInputOrderSensitive
//
//	@doc:
//		Is input order sensitive
//
//---------------------------------------------------------------------------
BOOL
CLogicalDynamicIndexGet::FInputOrderSensitive() const
{
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalDynamicIndexGet::PxfsCandidates
	(
	IMemoryPool *pmp
	)
const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfDynamicIndexGet2DynamicIndexScan);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::PstatsDerive
//
//	@doc:
//		Load up statistics from metadata
//
//---------------------------------------------------------------------------

IStatistics *
CLogicalDynamicIndexGet::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat *pdrgpstatCtxt
	)
	const
{
	return CStatisticsUtils::PstatsIndexGet(pmp, exprhdl, pdrgpstatCtxt);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalDynamicIndexGet::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalDynamicIndexGet::OsPrint
	(
	IOstream &os
	)
const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os << SzId() << " ";
	// index name
	os << "  Index Name: (";
	m_pindexdesc->Name().OsPrint(os);
	// table alias name
	os <<")";
	os << ", Table Name: (";
	m_pnameAlias->OsPrint(os);
	os <<"), ";
	m_ppartcnstr->OsPrint(os);
	os << ", Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrOutput);
	os << "] Scan Id: " << m_ulScanId << "." << m_ulSecondaryScanId;

	if (!m_ppartcnstr->FUnbounded())
	{
		os << ", ";
		m_ppartcnstr->OsPrint(os);
	}
	
	return os;
}

// EOF

