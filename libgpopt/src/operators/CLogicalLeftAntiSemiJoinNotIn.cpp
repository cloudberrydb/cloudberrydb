//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalLeftAntiSemiJoinNotIn.cpp
//
//	@doc:
//		Implementation of left anti semi join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/operators/CLogicalLeftAntiSemiJoinNotIn.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiJoinNotIn::CLogicalLeftAntiSemiJoinNotIn
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalLeftAntiSemiJoinNotIn::CLogicalLeftAntiSemiJoinNotIn
	(
	IMemoryPool *pmp
	)
	:
	CLogicalLeftAntiSemiJoin(pmp)
{
	GPOS_ASSERT(NULL != pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftAntiSemiJoinNotIn::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftAntiSemiJoinNotIn::PxfsCandidates
	(
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfAntiSemiJoinNotInAntiSemiJoinNotInSwap);
	(void) pxfs->FExchangeSet(CXform::ExfAntiSemiJoinNotInAntiSemiJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfAntiSemiJoinNotInSemiJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfAntiSemiJoinNotInInnerJoinSwap);
	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiJoinNotIn2CrossProduct);
	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiJoinNotIn2NLJoinNotIn);
	(void) pxfs->FExchangeSet(CXform::ExfLeftAntiSemiJoinNotIn2HashJoinNotIn);
	return pxfs;
}

// EOF
