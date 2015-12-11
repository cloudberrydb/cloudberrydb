//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CLogicalFullOuterJoin.cpp
//
//	@doc:
//		Implementation of full outer join operator
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
	IMemoryPool *pmp
	)
	:
	CLogicalJoin(pmp)
{
	GPOS_ASSERT(NULL != pmp);
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
	IMemoryPool *, // pmp
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
	IMemoryPool *pmp
	)
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);
	(void) pxfs->FExchangeSet(CXform::ExfExpandFullOuterJoin);
	return pxfs;
}

// EOF

