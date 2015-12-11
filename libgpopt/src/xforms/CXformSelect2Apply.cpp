//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CXformSelect2Apply.cpp
//
//	@doc:
//		Implementation of Select to Apply transform
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
#include "gpopt/xforms/CXformSelect2Apply.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2Apply::CXformSelect2Apply
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2Apply::CXformSelect2Apply
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
				GPOS_NEW(pmp) CLogicalSelect(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// predicate tree
				)
		)
{}

// EOF

