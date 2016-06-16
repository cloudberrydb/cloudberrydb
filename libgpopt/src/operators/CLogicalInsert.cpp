//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalInsert.cpp
//
//	@doc:
//		Implementation of logical Insert operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalInsert.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::CLogicalInsert
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalInsert::CLogicalInsert
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_ptabdesc(NULL),
	m_pdrgpcrSource(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::CLogicalInsert
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalInsert::CLogicalInsert
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrSource
	)
	:
	CLogical(pmp),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrSource(pdrgpcrSource)

{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrSource);

	m_pcrsLocalUsed->Include(m_pdrgpcrSource);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::~CLogicalInsert
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalInsert::~CLogicalInsert()
{
	CRefCount::SafeRelease(m_ptabdesc);
	CRefCount::SafeRelease(m_pdrgpcrSource);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalInsert::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalInsert *popInsert = CLogicalInsert::PopConvert(pop);

	return m_ptabdesc->Pmdid()->FEquals(popInsert->Ptabdesc()->Pmdid()) &&
			m_pdrgpcrSource->FEqual(popInsert->PdrgpcrSource());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalInsert::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrSource));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalInsert::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrSource, phmulcr, fMustExist);
	m_ptabdesc->AddRef();

	return GPOS_NEW(pmp) CLogicalInsert(pmp, m_ptabdesc, pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalInsert::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
{
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsOutput->Include(m_pdrgpcrSource);
	return pcrsOutput;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalInsert::PkcDeriveKeys
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
//		CLogicalInsert::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalInsert::Maxcard
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
//		CLogicalInsert::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalInsert::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfInsert2DML);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalInsert::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalInsert::PstatsDerive
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
//		CLogicalInsert::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalInsert::OsPrint
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
	os << "), Source Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrSource);
	os	<< "]";

	return os;
}

// EOF

