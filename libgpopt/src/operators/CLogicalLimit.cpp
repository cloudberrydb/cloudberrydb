//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLimit.cpp
//
//	@doc:
//		Implementation of logical limit operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/base/IDatumInt8.h"
#include "naucrates/md/IMDTypeInt8.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalLimit.h"

#include "naucrates/statistics/CLimitStatsProcessor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::CLogicalLimit
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalLimit::CLogicalLimit
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp),
	m_pos(NULL),
	m_fGlobal(true),
	m_fHasCount(false),
	m_fTopLimitUnderDML(false)
{
	m_fPattern = true;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::CLogicalLimit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalLimit::CLogicalLimit
	(
	IMemoryPool *pmp,
	COrderSpec *pos,
	BOOL fGlobal,
	BOOL fHasCount,
	BOOL fTopLimitUnderDML
	)
	:
	CLogical(pmp),
	m_pos(pos),
	m_fGlobal(fGlobal),
	m_fHasCount(fHasCount),
	m_fTopLimitUnderDML(fTopLimitUnderDML)
{
	GPOS_ASSERT(NULL != pos);
	CColRefSet *pcrsSort = m_pos->PcrsUsed(pmp);
	m_pcrsLocalUsed->Include(pcrsSort);
	pcrsSort->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::~CLogicalLimit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalLimit::~CLogicalLimit()
{
	CRefCount::SafeRelease(m_pos);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CLogicalLimit::UlHash() const
{
	return gpos::UlCombineHashes
			(
			gpos::UlCombineHashes(COperator::UlHash(), m_pos->UlHash()),
			gpos::UlCombineHashes(gpos::UlHash<BOOL>(&m_fGlobal), gpos::UlHash<BOOL>(&m_fHasCount))
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalLimit::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CLogicalLimit *popLimit = CLogicalLimit::PopConvert(pop);
		
		if (popLimit->FGlobal() == m_fGlobal &&
			popLimit->FHasCount() == m_fHasCount)
		{
			// match if order specs match
			return m_pos->FMatch(popLimit->m_pos);
		}
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalLimit::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	COrderSpec *pos = m_pos->PosCopyWithRemappedColumns(pmp, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalLimit(pmp, pos, m_fGlobal, m_fHasCount, m_fTopLimitUnderDML);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLimit::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PcrsDeriveOuter
//
//	@doc:
//		Derive outer references
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLimit::PcrsDeriveOuter
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	CColRefSet *pcrsSort = m_pos->PcrsUsed(pmp);
	CColRefSet *pcrsOuter = CLogical::PcrsDeriveOuter(pmp, exprhdl, pcrsSort);
	pcrsSort->Release();

	return pcrsOuter;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLimit::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	CExpression *pexprCount = exprhdl.PexprScalarChild(2 /*ulChildIndex*/);
	if (CUtils::FScalarConstInt<IMDTypeInt8>(pexprCount))
	{
		CScalarConst *popScalarConst = CScalarConst::PopConvert(pexprCount->Pop());
		IDatumInt8 *pdatumInt8 = dynamic_cast<IDatumInt8 *>(popScalarConst->Pdatum());

		return CMaxCard(pdatumInt8->LValue());
	}

	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLimit::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	
	(void) pxfs->FExchangeSet(CXform::ExfImplementLimit);
	(void) pxfs->FExchangeSet(CXform::ExfSplitLimit);
	
	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PcrsStat
//
//	@doc:
//		Compute required stat columns of the n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLimit::PcrsStat
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsInput,
	ULONG ulChildIndex
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	CColRefSet *pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
	// add columns used by number of rows and offset scalar children
	pcrsUsed->Union(exprhdl.Pdpscalar(1)->PcrsUsed());
	pcrsUsed->Union(exprhdl.Pdpscalar(2)->PcrsUsed());

	CColRefSet *pcrsStat = PcrsReqdChildStats(pmp, exprhdl, pcrsInput, pcrsUsed, ulChildIndex);
	pcrsUsed->Release();

	return pcrsStat;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalLimit::PkcDeriveKeys
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
//		CLogicalLimit::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CLogicalLimit::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " " << (*m_pos) << " " << (m_fGlobal ? "global" : "local")
			<< (m_fTopLimitUnderDML ? " NonRemovableLimit" : "");
	
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLimit::PstatsDerive
//
//	@doc:
//		Derive statistics based on limit
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalLimit::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *pstatsChild = exprhdl.Pstats(0);
	CMaxCard maxcard = this->Maxcard(pmp, exprhdl);
	CDouble dRowsMax = CDouble(maxcard.Ull());

	if (pstatsChild->DRows() <= dRowsMax)
	{
		pstatsChild->AddRef();
		return pstatsChild;
	}

	return CLimitStatsProcessor::PstatsLimit(pmp, dynamic_cast<CStatistics*>(pstatsChild), dRowsMax);
}

// EOF
