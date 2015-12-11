//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSwitchCase.cpp
//
//	@doc:
//		Implementation of scalar SwitchCase operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CScalarSwitchCase.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSwitchCase::CScalarSwitchCase
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSwitchCase::CScalarSwitchCase
	(
	IMemoryPool *pmp
	)
	:
	CScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSwitchCase::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarSwitchCase::FMatch
	(
	COperator *pop
	)
	const
{
	return (pop->Eopid() == Eopid());
}

// EOF

