//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementTVFNoArgs.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementTVFNoArgs.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CXformImplementTVFNoArgs::CXformImplementTVFNoArgs
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformImplementTVFNoArgs::CXformImplementTVFNoArgs
	(
	IMemoryPool *pmp
	)
	:
	CXformImplementTVF
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalTVF(pmp)
				)
		)
{}

// EOF
