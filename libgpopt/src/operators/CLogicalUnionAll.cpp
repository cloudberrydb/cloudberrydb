//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CLogicalUnionAll.cpp
//
//	@doc:
//		Implementation of UnionAll operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CLogicalUnionAll.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "naucrates/statistics/CUnionAllStatsProcessor.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::CLogicalUnionAll
//
//	@doc:
//		Ctor - for pattern
//
//---------------------------------------------------------------------------
CLogicalUnionAll::CLogicalUnionAll
	(
	IMemoryPool *pmp
	)
	:
	CLogicalUnion(pmp),
	m_ulScanIdPartialIndex(0)
{
	m_fPattern = true;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::CLogicalUnionAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalUnionAll::CLogicalUnionAll
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOutput,
	DrgDrgPcr *pdrgpdrgpcrInput,
	ULONG ulScanIdPartialIndex
	)
	:
	CLogicalUnion(pmp, pdrgpcrOutput, pdrgpdrgpcrInput),
	m_ulScanIdPartialIndex(ulScanIdPartialIndex)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::~CLogicalUnionAll
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CLogicalUnionAll::~CLogicalUnionAll()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalUnionAll::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.UlArity();

	CMaxCard maxcard = exprhdl.Pdprel(0)->Maxcard();
	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		maxcard += exprhdl.Pdprel(ul)->Maxcard();
	}

	return maxcard;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CLogicalUnionAll::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPcr *pdrgpcrOutput = CUtils::PdrgpcrRemap(pmp, m_pdrgpcrOutput, phmulcr, fMustExist);
	DrgDrgPcr *pdrgpdrgpcrInput = CUtils::PdrgpdrgpcrRemap(pmp, m_pdrgpdrgpcrInput, phmulcr, fMustExist);

	return GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrOutput, pdrgpdrgpcrInput, m_ulScanIdPartialIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalUnionAll::PkcDeriveKeys
	(
	IMemoryPool *, //pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalUnionAll::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfImplementUnionAll);

	return pxfs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PstatsDeriveUnionAll
//
//	@doc:
//		Derive statistics based on union all semantics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnionAll::PstatsDeriveUnionAll
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(COperator::EopLogicalUnionAll == exprhdl.Pop()->Eopid() || COperator::EopLogicalUnion == exprhdl.Pop()->Eopid());

	DrgPcr *pdrgpcrOutput = CLogicalSetOp::PopConvert(exprhdl.Pop())->PdrgpcrOutput();
	DrgDrgPcr *pdrgpdrgpcrInput = CLogicalSetOp::PopConvert(exprhdl.Pop())->PdrgpdrgpcrInput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	GPOS_ASSERT(NULL != pdrgpdrgpcrInput);

	IStatistics *pstatsResult = exprhdl.Pstats(0);
	pstatsResult->AddRef();
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		IStatistics *pstatsChild = exprhdl.Pstats(ul);
		CStatistics *pstats = CUnionAllStatsProcessor::PstatsUnionAll
											(
											pmp,
											dynamic_cast<CStatistics *>(pstatsResult),
											dynamic_cast<CStatistics *>(pstatsChild),
											CColRef::Pdrgpul(pmp, pdrgpcrOutput),
											CColRef::Pdrgpul(pmp, (*pdrgpdrgpcrInput)[0]),
											CColRef::Pdrgpul(pmp, (*pdrgpdrgpcrInput)[ul])
											);
		pstatsResult->Release();
		pstatsResult = pstats;
	}

	return pstatsResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalUnionAll::PstatsDerive
//
//	@doc:
//		Derive statistics based on union all semantics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalUnionAll::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	GPOS_ASSERT(EspNone < Esp(exprhdl));

	return PstatsDeriveUnionAll(pmp, exprhdl);
}

// EOF
