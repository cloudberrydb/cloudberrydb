//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalFullOuterJoin.cpp
//
//	@doc:
//		Implementation of full outer join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/operators/CLogicalFullOuterJoin.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalFullOuterJoin::CLogicalFullOuterJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalFullOuterJoin::CLogicalFullOuterJoin
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
//		CLogicalFullOuterJoin::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalFullOuterJoin::Maxcard
	(
	IMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, MaxcardDef(exprhdl));
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalFullOuterJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalFullOuterJoin::PxfsCandidates
	(
	IMemoryPool *mp
	)
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfExpandFullOuterJoin);
	return xform_set;
}

// EOF

