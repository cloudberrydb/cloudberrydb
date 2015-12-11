//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalSplit.cpp
//
//	@doc:
//		Implementation of logical split operator
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
#include "gpopt/operators/CLogicalSplit.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::CLogicalSplit
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalSplit::CLogicalSplit
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pdrgpcrDelete(NULL),
	m_pdrgpcrInsert(NULL),
	m_pcrCtid(NULL),
	m_pcrSegmentId(NULL),
	m_pcrAction(NULL),
	m_pcrTupleOid(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::CLogicalSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalSplit::CLogicalSplit
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrDelete,
	DrgPcr *pdrgpcrInsert,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId,
	CColRef *pcrAction,
	CColRef *pcrTupleOid
	)
	:
	CLogical(pmp),
	m_pdrgpcrDelete(pdrgpcrDelete),
	m_pdrgpcrInsert(pdrgpcrInsert),
	m_pcrCtid(pcrCtid),
	m_pcrSegmentId(pcrSegmentId),
	m_pcrAction(pcrAction),
	m_pcrTupleOid(pcrTupleOid)

{
	GPOS_ASSERT(NULL != pdrgpcrDelete);
	GPOS_ASSERT(NULL != pdrgpcrInsert);
	GPOS_ASSERT(pdrgpcrInsert->UlLength() == pdrgpcrDelete->UlLength());
	GPOS_ASSERT(NULL != pcrCtid);
	GPOS_ASSERT(NULL != pcrSegmentId);
	GPOS_ASSERT(NULL != pcrAction);

	m_pcrsLocalUsed->Include(m_pdrgpcrDelete);
	m_pcrsLocalUsed->Include(m_pdrgpcrInsert);
	m_pcrsLocalUsed->Include(m_pcrCtid);
	m_pcrsLocalUsed->Include(m_pcrSegmentId);
	m_pcrsLocalUsed->Include(m_pcrAction);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::~CLogicalSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalSplit::~CLogicalSplit()
{
	CRefCount::SafeRelease(m_pdrgpcrDelete);
	CRefCount::SafeRelease(m_pdrgpcrInsert);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL
CLogicalSplit::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CLogicalSplit *popSplit = CLogicalSplit::PopConvert(pop);

		return m_pcrCtid == popSplit->PcrCtid() &&
				m_pcrSegmentId == popSplit->PcrSegmentId() &&
				m_pcrAction == popSplit->PcrAction() &&
				m_pcrTupleOid == popSplit->PcrTupleOid() &&
				m_pdrgpcrDelete->FEqual(popSplit->PdrgpcrDelete()) &&
				m_pdrgpcrInsert->FEqual(popSplit->PdrgpcrInsert());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalSplit::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), CUtils::UlHashColArray(m_pdrgpcrInsert));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrCtid));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrSegmentId));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrAction));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalSplit::PopCopyWithRemappedColumns
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
	CColRef *pcrAction = CUtils::PcrRemap(m_pcrAction, phmulcr, fMustExist);
	
	CColRef *pcrTupleOid = NULL;
	if (NULL != m_pcrTupleOid)
	{
		pcrTupleOid = CUtils::PcrRemap(m_pcrTupleOid, phmulcr, fMustExist);
	}

	return GPOS_NEW(pmp) CLogicalSplit(pmp, pdrgpcrDelete, pdrgpcrInsert, pcrCtid, pcrSegmentId, pcrAction, pcrTupleOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalSplit::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(2 == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Union(exprhdl.Pdprel(0)->PcrsOutput());
	pcrs->Include(m_pcrAction);
	
	if (NULL != m_pcrTupleOid)
	{
		pcrs->Include(m_pcrTupleOid);
	}

	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalSplit::PkcDeriveKeys
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
//		CLogicalSplit::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalSplit::Maxcard
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
//		CLogicalSplit::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalSplit::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementSplit);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalSplit::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	// split returns double the number of tuples coming from its child
	IStatistics *pstats = exprhdl.Pstats(0);

	return pstats->PstatsScale(pmp, CDouble(2.0) /*dFactor*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalSplit::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalSplit::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os	<< SzId() << " -- Delete Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrDelete);
	os	<< "], Insert Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrInsert);
	os	<< "], ";
	m_pcrCtid->OsPrint(os);
	os	<< ", ";
	m_pcrSegmentId->OsPrint(os);
	os	<< ", ";
	m_pcrAction->OsPrint(os);

	return os;
}

// EOF

