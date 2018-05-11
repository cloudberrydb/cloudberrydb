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
	IMemoryPool *mp
	)
	:
	CLogicalJoin(mp)
{
	GPOS_ASSERT(NULL != mp);
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
	IMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, exprhdl.Arity() - 1, MaxcardDef(exprhdl));
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
	IMemoryPool *mp
	) 
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	
	(void) xform_set->ExchangeSet(CXform::ExfSubqNAryJoin2Apply);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoin);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinMinCard);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinDP);
	(void) xform_set->ExchangeSet(CXform::ExfExpandNAryJoinGreedy);

	return xform_set;
}


// EOF

