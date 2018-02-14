//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CScalarCoerceToDomain.cpp
//
//	@doc:
//		Implementation of scalar CoerceToDomain operators
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarCoerceToDomain.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceToDomain::CScalarCoerceToDomain
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCoerceToDomain::CScalarCoerceToDomain
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalarCoerceBase(pmp, pmdidType, iTypeModifier, ecf, iLoc),
	m_fReturnsNullOnNullInput(false)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceToDomain::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCoerceToDomain::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCoerceToDomain *popCoerce = CScalarCoerceToDomain::PopConvert(pop);

		return popCoerce->PmdidType()->FEquals(PmdidType()) &&
				popCoerce->ITypeModifier() == ITypeModifier() &&
				popCoerce->Ecf() == Ecf() &&
				popCoerce->ILoc() == ILoc();
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceToDomain::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarCoerceToDomain::Eber
	(
	DrgPul *pdrgpulChildren
	)
	const
{
	if (m_fReturnsNullOnNullInput)
	{
		return EberNullOnAnyNullChild(pdrgpulChildren);
	}

	return EberUnknown;
}


// EOF

