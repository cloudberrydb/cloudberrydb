//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalLeftOuterJoin.cpp
//
//	@doc:
//		Implementation of left outer join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "gpopt/operators/CLogicalLeftOuterJoin.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterJoin::CLogicalLeftOuterJoin
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CLogicalLeftOuterJoin::CLogicalLeftOuterJoin
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
//		CLogicalLeftOuterJoin::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalLeftOuterJoin::Maxcard
	(
	IMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{
	return CLogical::Maxcard(exprhdl, 2 /*ulScalarIndex*/, exprhdl.GetRelationalProperties(0)->Maxcard());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalLeftOuterJoin::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalLeftOuterJoin::PxfsCandidates
	(
	IMemoryPool *mp
	) 
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);

	(void) xform_set->ExchangeSet(CXform::ExfPushDownLeftOuterJoin);
	(void) xform_set->ExchangeSet(CXform::ExfSimplifyLeftOuterJoin);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterJoin2BitmapIndexGetApply);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterJoin2IndexGetApply);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterJoin2NLJoin);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterJoin2HashJoin);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuter2InnerUnionAllLeftAntiSemiJoin);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterJoinWithInnerSelect2BitmapIndexGetApply);
	(void) xform_set->ExchangeSet(CXform::ExfLeftOuterJoinWithInnerSelect2IndexGetApply);

	return xform_set;
}



// EOF

