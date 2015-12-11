//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalUpdate.cpp
//
//	@doc:
//		Implementation of logical Update operator
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
#include "gpopt/operators/CLogicalUpdate.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::CLogicalUpdate
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalUpdate::CLogicalUpdate
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ptabdesc(NULL),
	m_pdrgpcrDelete(NULL),
	m_pdrgpcrInsert(NULL),
	m_pcrCtid(NULL),
	m_pcrSegmentId(NULL),
	m_pcrTupleOid(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::CLogicalUpdate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalUpdate::CLogicalUpdate
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrDelete,
	DrgPcr *pdrgpcrInsert,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId, 
	CColRef *pcrTupleOid
	)
	:
	CLogical(pmp),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrDelete(pdrgpcrDelete),
	m_pdrgpcrInsert(pdrgpcrInsert),
	m_pcrCtid(pcrCtid),
	m_pcrSegmentId(pcrSegmentId),
	m_pcrTupleOid(pcrTupleOid)

{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrDelete);
	GPOS_ASSERT(NULL != pdrgpcrInsert);
	GPOS_ASSERT(pdrgpcrDelete->UlLength() == pdrgpcrInsert->UlLength());
	GPOS_ASSERT(NULL != pcrCtid);
	GPOS_ASSERT(NULL != pcrSegmentId);

	m_pcrsLocalUsed->Include(m_pdrgpcrDelete);
	m_pcrsLocalUsed->Include(m_pdrgpcrInsert);
	m_pcrsLocalUsed->Include(m_pcrCtid);
	m_pcrsLocalUsed->Include(m_pcrSegmentId);

	if (NULL != m_pcrTupleOid)
	{
		m_pcrsLocalUsed->Include(m_pcrTupleOid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::~CLogicalUpdate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalUpdate::~CLogicalUpdate()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrDelete);
	CRefCount::SafeRelease(m_pdrgpcrInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalUpdate::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalUpdate *popUpdate = CLogicalUpdate::PopConvert(pop);

	return m_pcrCtid == popUpdate->PcrCtid() &&
			m_pcrSegmentId == popUpdate->PcrSegmentId() &&
			m_pcrTupleOid == popUpdate->PcrTupleOid() &&
			m_ptabdesc->Pmdid()->FEquals(popUpdate->Ptabdesc()->Pmdid()) &&
			m_pdrgpcrDelete->FEqual(popUpdate->PdrgpcrDelete()) &&
			m_pdrgpcrInsert->FEqual(popUpdate->PdrgpcrInsert());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalUpdate::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrDelete));
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrInsert));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrCtid));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrSegmentId));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalUpdate::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrDelete = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrDelete, phmulcr, fMustExist);
	DrgPcr *pdrgpcrInsert = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrInsert, phmulcr, fMustExist);
	CColRef *pcrCtid = CUtils::PcrRemap(m_pcrCtid, phmulcr, fMustExist);
	CColRef *pcrSegmentId = CUtils::PcrRemap(m_pcrSegmentId, phmulcr, fMustExist);
	m_ptabdesc->AddRef();

	CColRef *pcrTupleOid = NULL;
	if (NULL != m_pcrTupleOid)
	{
		pcrTupleOid = CUtils::PcrRemap(m_pcrTupleOid, phmulcr, fMustExist);
	}
	return GPOS_NEW(pmp) CLogicalUpdate(pmp, m_ptabdesc, pdrgpcrDelete, pdrgpcrInsert, pcrCtid, pcrSegmentId, pcrTupleOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalUpdate::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsOutput->Include(m_pdrgpcrInsert);
	pcrsOutput->Include(m_pcrCtid);
	pcrsOutput->Include(m_pcrSegmentId);
	
	if (NULL != m_pcrTupleOid)
	{
		pcrsOutput->Include(m_pcrTupleOid);
	}
	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalUpdate::PkcDeriveKeys
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
//		CLogicalUpdate::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalUpdate::Maxcard
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
//		CLogicalUpdate::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalUpdate::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfUpdate2DML);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUpdate::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUpdate::PstatsDerive
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
//		CLogicalUpdate::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalUpdate::OsPrint
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
	os << "), Delete Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrDelete);
	os	<< "], Insert Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrInsert);
	os	<< "], ";
	m_pcrCtid->OsPrint(os);
	os	<< ", ";
	m_pcrSegmentId->OsPrint(os);

	return os;
}

// EOF

