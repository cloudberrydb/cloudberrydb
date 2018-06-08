//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalNAryJoin.cpp
//
//	@doc:
//		Implementation of n-ary inner join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CLogicalNAryJoin.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::CLogicalNAryJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalNAryJoin::CLogicalNAryJoin
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
//		CLogicalNAryJoin::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalNAryJoin::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, exprhdl.UlArity() - 1, MaxcardDef(exprhdl));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalNAryJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalNAryJoin::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	
	(void) pxfs->FExchangeSet(CXform::ExfSubqNAryJoin2Apply);
	(void) pxfs->FExchangeSet(CXform::ExfExpandNAryJoin);
	(void) pxfs->FExchangeSet(CXform::ExfExpandNAryJoinMinCard);
	(void) pxfs->FExchangeSet(CXform::ExfExpandNAryJoinDP);
	(void) pxfs->FExchangeSet(CXform::ExfExpandNAryJoinGreedy);

	return pxfs;
}


// EOF

