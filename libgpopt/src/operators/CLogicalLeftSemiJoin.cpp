	//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftSemiJoin.cpp
//
//	@doc:
//		Implementation of left semi join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalLeftSemiJoin.h"

#include "naucrates/statistics/CStatsPredUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiJoin::CLogicalLeftSemiJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalLeftSemiJoin::CLogicalLeftSemiJoin
	(
	IMemoryPool *pmp
	)
	:
	CLogicalJoin(pmp)
{
	GPOS_ASSERT(NULL != pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftSemiJoin::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfSemiJoinSemiJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfSemiJoinAntiSemiJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfSemiJoinAntiSemiJoinNotInSwap);
	(void) pxfs->FExchangeSet(CXform::ExfSemiJoinInnerJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiJoin2InnerJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiJoin2InnerJoinUnderGb);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiJoin2CrossProduct);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiJoin2NLJoin);
	(void) pxfs->FExchangeSet(CXform::ExfLeftSemiJoin2HashJoin);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiJoin::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalLeftSemiJoin::PcrsDeriveOutput
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(3 == exprhdl.UlArity());

	return PcrsDeriveOutputPassThru(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiJoin::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalLeftSemiJoin::PkcDeriveKeys
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
//		CLogicalLeftSemiJoin::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftSemiJoin::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, exprhdl.Pdprel(0)->Maxcard());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiJoin::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalLeftSemiJoin::PstatsDerive
	(
	IMemoryPool *pmp,
	DrgPstatspredjoin *pdrgpstatspredjoin,
	IStatistics *pstatsOuter,
	IStatistics *pstatsInner
	)
{
	return pstatsOuter->PstatsLSJoin(pmp, pstatsInner, pdrgpstatspredjoin);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftSemiJoin::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalLeftSemiJoin::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // not used
	)
	const
{
	GPOS_ASSERT(Esp(exprhdl) > EspNone);
	IStatistics *pstatsOuter = exprhdl.Pstats(0);
	IStatistics *pstatsInner = exprhdl.Pstats(1);
	DrgPstatspredjoin *pdrgpstatspredjoin = CStatsPredUtils::Pdrgpstatspredjoin(pmp, exprhdl);
	IStatistics *pstatsSemiJoin = PstatsDerive(pmp, pdrgpstatspredjoin, pstatsOuter, pstatsInner);

	pdrgpstatspredjoin->Release();

	return pstatsSemiJoin;
}
// EOF
