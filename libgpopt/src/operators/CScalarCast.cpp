//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarCast.cpp
//
//	@doc:
//		Implementation of scalar relabel type  operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarCast.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarCast::CScalarCast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCast::CScalarCast
	(
	IMemoryPool *pmp,
	IMDId *pmdidReturnType,
	IMDId *pmdidFunc,
	BOOL fBinaryCoercible
	)
	:
	CScalar(pmp),
	m_pmdidReturnType(pmdidReturnType),
	m_pmdidFunc(pmdidFunc),
	m_fBinaryCoercible(fBinaryCoercible),
	m_fReturnsNullOnNullInput(false),
	m_fBoolReturnType(false)
{
	if (NULL != m_pmdidFunc && m_pmdidFunc->FValid())
	{
		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
		const IMDFunction *pmdfunc = pmda->Pmdfunc(m_pmdidFunc);

		m_fReturnsNullOnNullInput = pmdfunc->FStrict();
		m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidReturnType);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCast::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCast::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCast *pscop = CScalarCast::PopConvert(pop);

		// match if the return type oids are identical
		return pscop->PmdidType()->FEquals(m_pmdidReturnType) &&
				((!IMDId::FValid(pscop->PmdidFunc()) && !IMDId::FValid(m_pmdidFunc)) || pscop->PmdidFunc()->FEquals(m_pmdidFunc));
	}

	return false;
}

// EOF

