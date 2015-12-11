//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalCTEProducer.cpp
//
//	@doc:
//		Implementation of CTE producer operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalCTEProducer.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::CLogicalCTEProducer
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalCTEProducer::CLogicalCTEProducer
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ulId(0),
	m_pdrgpcr(NULL),
	m_pcrsOutput(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::CLogicalCTEProducer
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalCTEProducer::CLogicalCTEProducer
	(
	IMemoryPool *pmp,
	ULONG ulId,
	DrgPcr *pdrgpcr
	)
	:
	CLogical(pmp),
	m_ulId(ulId),
	m_pdrgpcr(pdrgpcr)
{
	GPOS_ASSERT(NULL != pdrgpcr);

	m_pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcr);
	GPOS_ASSERT(m_pdrgpcr->UlLength() == m_pcrsOutput->CElements());

	m_pcrsLocalUsed->Include(m_pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::~CLogicalCTEProducer
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalCTEProducer::~CLogicalCTEProducer()
{
	CRefCount::SafeRelease(m_pdrgpcr);
	CRefCount::SafeRelease(m_pcrsOutput);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEProducer::PcrsDeriveOutput
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & //exprhdl
	)
{
	m_pcrsOutput->AddRef();
	return m_pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PcrsDeriveNotNull
//
//	@doc:
//		Derive not nullable output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalCTEProducer::PcrsDeriveNotNull
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcr);
	pcrs->Intersection(exprhdl.Pdprel(0)->PcrsNotNull());

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalCTEProducer::PkcDeriveKeys
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
//		CLogicalCTEProducer::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalCTEProducer::Maxcard
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
//		CLogicalCTEProducer::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalCTEProducer::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalCTEProducer *popCTEProducer = CLogicalCTEProducer::PopConvert(pop);

	return m_ulId == popCTEProducer->UlCTEId() &&
			m_pdrgpcr->FEqual(popCTEProducer->Pdrgpcr());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalCTEProducer::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ulId);
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcr));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalCTEProducer::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcr, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalCTEProducer(pmp, m_ulId, pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalCTEProducer::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementCTEProducer);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalCTEProducer::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalCTEProducer::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << m_ulId;
	os << "), Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcr);
	os	<< "]";

	return os;
}

// EOF
