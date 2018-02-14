//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Inc.
//
//	@filename:
//		CScalarCoerceBase.cpp
//
//	@doc:
//		Implementation of scalar coerce operator base class
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarCoerceBase.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::CScalarCoerceBase
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCoerceBase::CScalarCoerceBase
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalar(pmp),
	m_pmdidResultType(pmdidType),
	m_iTypeModifier(iTypeModifier),
	m_ecf(ecf),
	m_iLoc(iLoc)
{
	GPOS_ASSERT(NULL != pmdidType);
	GPOS_ASSERT(pmdidType->FValid());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::~CScalarCoerceBase
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarCoerceBase::~CScalarCoerceBase()
{
	m_pmdidResultType->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::PmdidType
//
//	@doc:
//		Return type of the scalar expression
//
//---------------------------------------------------------------------------
IMDId*
CScalarCoerceBase::PmdidType() const
{
	return m_pmdidResultType;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::ITypeModifier
//
//	@doc:
//		Return type modifier
//
//---------------------------------------------------------------------------
INT
CScalarCoerceBase::ITypeModifier() const
{
	return m_iTypeModifier;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::Ecf
//
//	@doc:
//		Return coercion form
//
//---------------------------------------------------------------------------
CScalar::ECoercionForm
CScalarCoerceBase::Ecf() const
{
	return m_ecf;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::ILoc
//
//	@doc:
//		Return token location
//
//---------------------------------------------------------------------------
INT
CScalarCoerceBase::ILoc() const
{
	return m_iLoc;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::PopCopyWithRemappedColumns
//
//	@doc:
//		return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator*
CScalarCoerceBase::PopCopyWithRemappedColumns
	(
	IMemoryPool *, //pmp,
	HMUlCr *, //phmulcr,
	BOOL //fMustExist
	)
{
	return PopCopyDefault();
}

// EOF

