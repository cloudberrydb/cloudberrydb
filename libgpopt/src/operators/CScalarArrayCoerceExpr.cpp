//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CScalarArrayCoerceExpr.cpp
//
//	@doc:
//		Implementation of scalar array coerce expr operator
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarArrayCoerceExpr.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::CScalarArrayCoerceExpr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarArrayCoerceExpr::CScalarArrayCoerceExpr
	(
	IMemoryPool *pmp,
	IMDId *pmdidElementFunc,
	IMDId *pmdidResultType,
	INT iTypeModifier,
	BOOL fIsExplicit,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalarCoerceBase(pmp, pmdidResultType, iTypeModifier, ecf, iLoc),
	m_pmdidElementFunc(pmdidElementFunc),
	m_fIsExplicit(fIsExplicit)
{
	GPOS_ASSERT(NULL != pmdidElementFunc);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::~CScalarArrayCoerceExpr
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CScalarArrayCoerceExpr::~CScalarArrayCoerceExpr()
{
	m_pmdidElementFunc->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::PmdidElementFunc
//
//	@doc:
//		Return metadata id of element coerce function
//
//---------------------------------------------------------------------------
IMDId *
CScalarArrayCoerceExpr::PmdidElementFunc() const
{
	return m_pmdidElementFunc;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::FIsExplicit
//
//	@doc:
//		Conversion semantics flag to pass to func
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayCoerceExpr::FIsExplicit() const
{
	return m_fIsExplicit;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::Eopid
//
//	@doc:
//		Return operator identifier
//
//---------------------------------------------------------------------------
CScalar::EOperatorId
CScalarArrayCoerceExpr::Eopid() const
{
	return EopScalarArrayCoerceExpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::SzId
//
//	@doc:
//		Return a string for operator name
//
//---------------------------------------------------------------------------
const CHAR *
CScalarArrayCoerceExpr::SzId() const
{
	return "CScalarArrayCoerceExpr";
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayCoerceExpr::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CScalarArrayCoerceExpr *popCoerce = CScalarArrayCoerceExpr::PopConvert(pop);

	return popCoerce->PmdidElementFunc()->FEquals(m_pmdidElementFunc) &&
			popCoerce->PmdidType()->FEquals(PmdidType()) &&
			popCoerce->ITypeModifier() == ITypeModifier() &&
			popCoerce->FIsExplicit() == m_fIsExplicit &&
			popCoerce->Ecf() == Ecf() &&
			popCoerce->ILoc() == ILoc();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to order of inputs
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayCoerceExpr::FInputOrderSensitive() const
{
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayCoerceExpr::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CScalarArrayCoerceExpr *
CScalarArrayCoerceExpr::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopScalarArrayCoerceExpr == pop->Eopid());

	return dynamic_cast<CScalarArrayCoerceExpr*>(pop);
}


// EOF

