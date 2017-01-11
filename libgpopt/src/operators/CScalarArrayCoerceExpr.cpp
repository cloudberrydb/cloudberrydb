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

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarArrayCoerceExpr.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

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
	CScalar(pmp),
	m_pmdidElementFunc(pmdidElementFunc),
	m_pmdidResultType(pmdidResultType),
	m_iMod(iMod),
	m_fIsExplicit(fIsExplicit),
	m_ecf(ecf),
	m_iLoc(iLoc)
{
	GPOS_ASSERT(NULL != pmdidElementFunc);
	
	GPOS_ASSERT(NULL != pmdidResultType);
	GPOS_ASSERT(pmdidResultType->FValid());
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
			popCoerce->PmdidType()->FEquals(m_pmdidResultType) &&
			popCoerce->IMod() == m_iMod &&
			popCoerce->FIsExplicit() == m_fIsExplicit &&
			popCoerce->Ecf() == m_ecf &&
			popCoerce->ILoc() == m_iLoc;
}

// EOF

