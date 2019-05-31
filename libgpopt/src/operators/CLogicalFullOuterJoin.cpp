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
	CMemoryPool *mp
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
	CMemoryPool *, // mp
	CExpressionHandle &exprhdl
	)
	const
{

	CMaxCard left_child_maxcard = exprhdl.GetRelationalProperties(0)->Maxcard();
	CMaxCard right_child_maxcard = exprhdl.GetRelationalProperties(1)->Maxcard();

	if (left_child_maxcard.Ull() > 0 && right_child_maxcard.Ull() > 0)
	{
		CMaxCard result_max_card = left_child_maxcard;
		result_max_card *= right_child_maxcard;
		return result_max_card;
	}

	if (left_child_maxcard <= right_child_maxcard)
	{
		return right_child_maxcard;
	}

	return left_child_maxcard;
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
	CMemoryPool *mp
	)
	const
{
	CXformSet *xform_set = GPOS_NEW(mp) CXformSet(mp);
	(void) xform_set->ExchangeSet(CXform::ExfExpandFullOuterJoin);
	return xform_set;
}

// EOF

