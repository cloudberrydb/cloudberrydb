//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSequenceProject2Apply.cpp
//
//	@doc:
//		Implementation of Sequence Project to Apply transform
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformSequenceProject2Apply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSequenceProject2Apply::CXformSequenceProject2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSequenceProject2Apply::CXformSequenceProject2Apply
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
				GPOS_NEW(pmp) CLogicalSequenceProject(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),	// relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// project list
				)
		)
{}

// EOF

