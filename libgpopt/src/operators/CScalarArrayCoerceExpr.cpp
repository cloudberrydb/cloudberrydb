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
	INT iMod,
	BOOL fIsExplicit,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalarCoerceBase(pmp, pmdidResultType, iMod, ecf, iLoc),
	m_pmdidElementFunc(pmdidElementFunc),
	m_fIsExplicit(fIsExplicit)
{
	GPOS_ASSERT(NULL != pmdidElementFunc);
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
			popCoerce->IMod() == IMod() &&
			popCoerce->FIsExplicit() == m_fIsExplicit &&
			popCoerce->Ecf() == Ecf() &&
			popCoerce->ILoc() == ILoc();
}

// EOF

