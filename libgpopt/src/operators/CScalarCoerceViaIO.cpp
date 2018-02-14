//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CScalarCoerceViaIO.cpp
//
//	@doc:
//		Implementation of scalar CoerceViaIO operators
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarCoerceViaIO.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceViaIO::CScalarCoerceViaIO
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCoerceViaIO::CScalarCoerceViaIO
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalarCoerceBase(pmp, pmdidType, iTypeModifier, ecf, iLoc)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceViaIO::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCoerceViaIO::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCoerceViaIO *popCoerce = CScalarCoerceViaIO::PopConvert(pop);

		return popCoerce->PmdidType()->FEquals(PmdidType()) &&
				popCoerce->ITypeModifier() == ITypeModifier() &&
				popCoerce->Ecf() == Ecf() &&
				popCoerce->ILoc() == ILoc();
	}

	return false;
}


// EOF

