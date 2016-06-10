//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformGbAgg2Apply.cpp
//
//	@doc:
//		Implementation of GbAgg to Apply transform
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformGbAgg2Apply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Apply::CXformGbAgg2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformGbAgg2Apply::CXformGbAgg2Apply
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformSubqueryUnnest
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAgg(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),	// relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// project list
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformGbAgg2Apply::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		scalar child must have subquery
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformGbAgg2Apply::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popGbAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());
	if (popGbAgg->FGlobal() && exprhdl.Pdpscalar(1)->FHasSubquery())
	{
		return CXform::ExfpHigh;
	}

	return CXform::ExfpNone;
}


// EOF

