//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformProject2Apply.cpp
//
//	@doc:
//		Implementation of Project to Apply transform
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
#include "gpopt/xforms/CXformProject2Apply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformProject2Apply::CXformProject2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformProject2Apply::CXformProject2Apply
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
				GPOS_NEW(pmp) CLogicalProject(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)),	// relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// scalar project list
				)
		)
{}


// EOF

