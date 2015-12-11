//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalDelete.cpp
//
//	@doc:
//		Implementation of logical Delete operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalDelete.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::CLogicalDelete
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalDelete::CLogicalDelete
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ptabdesc(NULL),
	m_pdrgpcr(NULL),
	m_pcrCtid(NULL),
	m_pcrSegmentId(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::CLogicalDelete
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalDelete::CLogicalDelete
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcr,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId
	)
	:
	CLogical(pmp),
	m_ptabdesc(ptabdesc),
	m_pdrgpcr(pdrgpcr),
	m_pcrCtid(pcrCtid),
	m_pcrSegmentId(pcrSegmentId)

{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(NULL != pcrCtid);
	GPOS_ASSERT(NULL != pcrSegmentId);

	m_pcrsLocalUsed->Include(m_pdrgpcr);
	m_pcrsLocalUsed->Include(m_pcrCtid);
	m_pcrsLocalUsed->Include(m_pcrSegmentId);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::~CLogicalDelete
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalDelete::~CLogicalDelete()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalDelete::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalDelete *popDelete = CLogicalDelete::PopConvert(pop);

	return m_pcrCtid == popDelete->PcrCtid() &&
			m_pcrSegmentId == popDelete->PcrSegmentId() &&
			m_ptabdesc->Pmdid()->FEquals(popDelete->Ptabdesc()->Pmdid()) &&
			m_pdrgpcr->FEqual(popDelete->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalDelete::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrCtid));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrSegmentId));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalDelete::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcr, phmulcr, fMustExist);
	CColRef *pcrCtid = CUtils::PcrRemap(m_pcrCtid, phmulcr, fMustExist);
	CColRef *pcrSegmentId = CUtils::PcrRemap(m_pcrSegmentId, phmulcr, fMustExist);
	m_ptabdesc->AddRef();

	return GPOS_NEW(pmp) CLogicalDelete(pmp, m_ptabdesc, pdrgpcr, pcrCtid, pcrSegmentId);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalDelete::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsOutput->Include(m_pdrgpcr);
	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalDelete::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalDelete::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalDelete::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfDelete2DML);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalDelete::PstatsDerive
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	return PstatsPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalDelete::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalDelete::OsPrint
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
		<< " (";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Deleted Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os	<< "], ";
	m_pcrCtid->OsPrint(os);
	os	<< ", ";
	m_pcrSegmentId->OsPrint(os);

	return os;
}

// EOF

