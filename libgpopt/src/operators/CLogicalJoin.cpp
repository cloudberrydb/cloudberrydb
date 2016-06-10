//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalJoin.cpp
//
//	@doc:
//		Implementation of logical join class
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalJoin.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::CLogicalJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalJoin::CLogicalJoin
	(
	IMemoryPool *pmp
	)
	:
	CLogical(pmp)
{
	GPOS_ASSERT(NULL != pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CLogicalJoin::FMatch
	(
	COperator *pop
	)
	const
{
	return (pop->Eopid() == Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalJoin::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalJoin::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat *pdrgpstatCtxt
	)
	const
{
	return CStatisticsUtils::PstatsJoin(pmp, exprhdl, pdrgpstatCtxt);
}

// EOF

