//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CScalarCmp.cpp
//
//	@doc:
//		Implementation of scalar comparison operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarCmp.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::CScalarCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCmp::CScalarCmp
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	const CWStringConst *pstrOp,
	IMDType::ECmpType ecmpt
	)
	:
	CScalar(pmp),
	m_pmdidOp(pmdidOp),
	m_pstrOp(pstrOp),
	m_ecmpt(ecmpt),
	m_fReturnsNullOnNullInput(false)
{
	GPOS_ASSERT(pmdidOp->FValid());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_fReturnsNullOnNullInput = CMDAccessorUtils::FScalarOpReturnsNullOnNullInput(pmda, m_pmdidOp);
	m_fCommutative = CMDAccessorUtils::FCommutativeScalarOp(pmda, m_pmdidOp);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::Pstr
//
//	@doc:
//		Comparison operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarCmp::Pstr() const
{
	return m_pstrOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::PmdidOp
//
//	@doc:
//		Comparison operator metadata id
//
//---------------------------------------------------------------------------
IMDId *
CScalarCmp::PmdidOp() const
{
	return m_pmdidOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		metadata id
//
//---------------------------------------------------------------------------
ULONG
CScalarCmp::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(), m_pmdidOp->UlHash());
}

	
//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCmp::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pop);
		
		// match if operator oid are identical
		return m_pmdidOp->FEquals(popScCmp->PmdidOp());
	}
	
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::FInputOrderSensitive
//
//	@doc:
//		Sensitivity to order of inputs
//
//---------------------------------------------------------------------------
BOOL
CScalarCmp::FInputOrderSensitive() const
{
	return !m_fCommutative;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarCmp::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarCmp::Eber
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


//---------------------------------------------------------------------------
//	@function:
//		CScalarCmp::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarCmp::OsPrint
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


// EOF

