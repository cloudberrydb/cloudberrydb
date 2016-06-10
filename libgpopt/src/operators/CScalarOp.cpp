//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarOp.cpp
//
//	@doc:
//		Implementation of general scalar operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDScalarOp.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarOp.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::CScalarOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarOp::CScalarOp
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	IMDId *pmdidReturnType,
	const CWStringConst *pstrOp
	)
	:
	CScalar(pmp),
	m_pmdidOp(pmdidOp),
	m_pmdidReturnType(pmdidReturnType),
	m_pstrOp(pstrOp),
	m_fReturnsNullOnNullInput(false),
	m_fBoolReturnType(false),
	m_fCommutative(false)
{
	GPOS_ASSERT(pmdidOp->FValid());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	m_fReturnsNullOnNullInput = CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(pmda, m_pmdidOp);
	m_fCommutative = CMDAccessorUtils::FCommutativeScalarOp(pmda, m_pmdidOp);
	m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidReturnType);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::Pstr
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarOp::Pstr() const
{
	return m_pstrOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::PmdidOp
//
//	@doc:
//		Scalar operator metadata id
//
//---------------------------------------------------------------------------
IMDId *
CScalarOp::PmdidOp() const
{
	return m_pmdidOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		metadata id
//
//---------------------------------------------------------------------------
ULONG
CScalarOp::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(), m_pmdidOp->UlHash());
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarOp::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarOp *pscop = CScalarOp::PopConvert(pop);

		// match if operator oid are identical
		return m_pmdidOp->FEquals(pscop->PmdidOp());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::PmdidReturnType
//
//	@doc:
//		Accessor to the return type
//
//---------------------------------------------------------------------------
IMDId *
CScalarOp::PmdidReturnType() const
{
	return m_pmdidReturnType;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarOp::PmdidType() const
{
	if (NULL != m_pmdidReturnType)
	{
		return m_pmdidReturnType;
	}
	
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->Pmdscop(m_pmdidOp)->PmdidTypeResult();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to order of inputs
//
//---------------------------------------------------------------------------
BOOL
CScalarOp::FInputOrderSensitive() const
{
	return !m_fCommutative;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarOp::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << Pstr()->Wsz();
	os << ")";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarOp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarOp::Eber
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

