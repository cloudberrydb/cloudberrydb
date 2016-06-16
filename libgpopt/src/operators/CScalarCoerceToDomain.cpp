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

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarCoerceToDomain.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

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
	INT iMod,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalar(pmp),
	m_pmdidResultType(pmdidType),
	m_iMod(iMod),
	m_ecf(ecf),
	m_iLoc(iLoc),
	m_fReturnsNullOnNullInput(false)
{
	GPOS_ASSERT(NULL != pmdidType);
	GPOS_ASSERT(pmdidType->FValid());

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

		return popCoerce->PmdidType()->FEquals(m_pmdidResultType) &&
				popCoerce->IMod() == m_iMod &&
				popCoerce->Ecf() == m_ecf &&
				popCoerce->ILoc() == m_iLoc;
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

