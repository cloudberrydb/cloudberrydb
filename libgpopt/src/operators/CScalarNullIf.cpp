//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarNullIf.cpp
//
//	@doc:
//		Implementation of scalar NullIf operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarNullIf.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarNullIf::CScalarNullIf
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarNullIf::CScalarNullIf
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	IMDId *pmdidType
	)
	:
	CScalar(pmp),
	m_pmdidOp(pmdidOp),
	m_pmdidType(pmdidType),
	m_fReturnsNullOnNullInput(false),
	m_fBoolReturnType(false)
{
	GPOS_ASSERT(pmdidOp->FValid());
	GPOS_ASSERT(pmdidType->FValid());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fReturnsNullOnNullInput = CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(pmda, m_pmdidOp);
	m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidType);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullIf::~CScalarNullIf
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarNullIf::~CScalarNullIf()
{
	m_pmdidOp->Release();
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarNullIf::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarNullIf::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(),
			gpos::UlCombineHashes(m_pmdidOp->UlHash(),m_pmdidType->UlHash()));
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarNullIf::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarNullIf::FMatch
	(
	COperator *pop
	)
	const
{
	if(pop->Eopid() != Eopid())
	{
		return false;
	}

	CScalarNullIf *popScNullIf = CScalarNullIf::PopConvert(pop);

	// match if operators and return types are identical
	return m_pmdidOp->FEquals(popScNullIf->PmdidOp()) &&
			m_pmdidType->FEquals(popScNullIf->PmdidType());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullIf::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarNullIf::Eber
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

