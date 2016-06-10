//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CLogicalGbAggDeduplicate.cpp
//
//	@doc:
//		Implementation of aggregate operator for deduplicating semijoin outputs
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalGbAggDeduplicate.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::CLogicalGbAggDeduplicate
//
//	@doc:
//		Ctor for xform pattern
//
//---------------------------------------------------------------------------
CLogicalGbAggDeduplicate::CLogicalGbAggDeduplicate
	(
	IMemoryPool *pmp
	)
	:
	CLogicalGbAgg(pmp),
	m_pdrgpcrKeys(NULL)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::CLogicalGbAggDeduplicate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalGbAggDeduplicate::CLogicalGbAggDeduplicate
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	COperator::EGbAggType egbaggtype,
	DrgPcr *pdrgpcrKeys
	)
	:
	CLogicalGbAgg(pmp, pdrgpcr, egbaggtype),
	m_pdrgpcrKeys(pdrgpcrKeys)
{
	GPOS_ASSERT(NULL != pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::CLogicalGbAggDeduplicate
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalGbAggDeduplicate::CLogicalGbAggDeduplicate
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	DrgPcr *pdrgpcrMinimal,
	COperator::EGbAggType egbaggtype,
	DrgPcr *pdrgpcrKeys
	)
	:
	CLogicalGbAgg(pmp, pdrgpcr, pdrgpcrMinimal, egbaggtype),
	m_pdrgpcrKeys(pdrgpcrKeys)
{
	GPOS_ASSERT(NULL != pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::~CLogicalGbAggDeduplicate
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalGbAggDeduplicate::~CLogicalGbAggDeduplicate()
{
	// safe release -- to allow for instances used in patterns
	CRefCount::SafeRelease(m_pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalGbAggDeduplicate::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcr = CUtils::PdrgpcrRemap(pmp, Pdrgpcr(), phmulcr, fMustExist);

	DrgPcr *pdrgpcrMinimal = PdrgpcrMinimal();
	if (NULL != pdrgpcrMinimal)
	{
		pdrgpcrMinimal = CUtils::PdrgpcrRemap(pmp, pdrgpcrMinimal, phmulcr, fMustExist);
	}

	DrgPcr *pdrgpcrKeys = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrKeys, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp, pdrgpcr, pdrgpcrMinimal, Egbaggtype(), pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::PcrsStat
//
//	@doc:
//		Compute required stats columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalGbAggDeduplicate::PcrsStat
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	ULONG ulChildIndex
	)
	const
{
	return PcrsStatGbAgg(pmp, exprhdl, pcrsInput, ulChildIndex, m_pdrgpcrKeys);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalGbAggDeduplicate::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), CUtils::UlHashColArray(Pdrgpcr()));
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrKeys));

	ULONG ulGbaggtype = (ULONG) Egbaggtype();

	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHash<ULONG>(&ulGbaggtype));

	return  gpos::UlCombineHashes(ulHash, gpos::UlHash<BOOL>(&m_fGeneratesDuplicates));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalGbAggDeduplicate::PkcDeriveKeys
	(
	IMemoryPool *pmp,
	CExpressionHandle & //exprhdl
	)
	const
{
	CKeyCollection *pkc = NULL;

	// Gb produces a key only if it's global
	if (FGlobal())
	{
		// keys from join child are still keys
		m_pdrgpcrKeys->AddRef();
		pkc = GPOS_NEW(pmp) CKeyCollection(pmp, m_pdrgpcrKeys);
	}

	return pkc;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalGbAggDeduplicate::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CLogicalGbAggDeduplicate *popAgg = CLogicalGbAggDeduplicate::PopConvert(pop);

	if (FGeneratesDuplicates() != popAgg->FGeneratesDuplicates())
	{
		return false;
	}

	return popAgg->Egbaggtype() == Egbaggtype() &&
			Pdrgpcr()->FEqual(popAgg->Pdrgpcr()) &&
			m_pdrgpcrKeys->FEqual(popAgg->PdrgpcrKeys()) &&
			CColRef::FEqual(PdrgpcrMinimal(), popAgg->PdrgpcrMinimal());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalGbAggDeduplicate::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfPushGbDedupBelowJoin);
	(void) pxfs->FExchangeSet(CXform::ExfSplitGbAggDedup);
	(void) pxfs->FExchangeSet(CXform::ExfGbAggDedup2HashAggDedup);
	(void) pxfs->FExchangeSet(CXform::ExfGbAggDedup2StreamAggDedup);
	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalGbAggDeduplicate::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *pstatsChild = exprhdl.Pstats(0);

	// extract computed columns
	DrgPul *pdrgpulComputedCols = GPOS_NEW(pmp) DrgPul(pmp);
	exprhdl.Pdpscalar(1 /*ulChildIndex*/)->PcrsDefined()->ExtractColIds(pmp, pdrgpulComputedCols);

	// construct bitset with keys of join child
	CBitSet *pbsKeys = GPOS_NEW(pmp) CBitSet(pmp);
	const ULONG ulKeys = m_pdrgpcrKeys->UlLength();
	for (ULONG ul = 0; ul < ulKeys; ul++)
	{
		CColRef *pcr = (*m_pdrgpcrKeys)[ul];
		pbsKeys->FExchangeSet(pcr->UlId());
	}

	IStatistics *pstats = CLogicalGbAgg::PstatsDerive(pmp, pstatsChild, Pdrgpcr(), pdrgpulComputedCols, pbsKeys);
	pbsKeys->Release();
	pdrgpulComputedCols->Release();

	return pstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalGbAggDeduplicate::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalGbAggDeduplicate::OsPrint
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
	OsPrintGbAggType(os, Egbaggtype());
	os	<< " )";
	os	<< " Grp Cols: [";
	CUtils::OsPrintDrgPcr(os, Pdrgpcr());
	os	<< "], Minimal Grp Cols: [";
	if (NULL != PdrgpcrMinimal())
	{
		CUtils::OsPrintDrgPcr(os, PdrgpcrMinimal());
	}
	os << "], Join Child Keys: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrKeys);
	os	<< "]";
	os	<< ", Generates Duplicates :[ " << FGeneratesDuplicates() << " ] ";
	
	return os;
}

// EOF
