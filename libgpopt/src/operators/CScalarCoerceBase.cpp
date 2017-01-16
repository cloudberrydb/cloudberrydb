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
	INT iMod,
	ECoercionForm ecf,
	INT iLoc
	)
	:
	CScalar(pmp),
	m_pmdidResultType(pmdidType),
	m_iMod(iMod),
	m_ecf(ecf),
	m_iLoc(iLoc)
{
	GPOS_ASSERT(NULL != pmdidType);
	GPOS_ASSERT(pmdidType->FValid());

}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCoerceBase::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCoerceBase::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCoerceBase *popCoerce = dynamic_cast<CScalarCoerceBase*>(pop);

		return popCoerce->PmdidType()->FEquals(m_pmdidResultType) &&
				popCoerce->IMod() == m_iMod &&
				popCoerce->Ecf() == m_ecf &&
				popCoerce->ILoc() == m_iLoc;
	}

	return false;
}


// EOF

