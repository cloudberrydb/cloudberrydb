//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarFunc.cpp
//
//	@doc:
//		Implementation of scalar function call operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDFunction.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/mdcache/CMDAccessorUtils.h"

#include "gpopt/operators/CScalarFunc.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::CScalarFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarFunc::CScalarFunc
	(
	IMemoryPool *pmp
	)
	:
	CScalar(pmp),
	m_pmdidFunc(NULL),
	m_pmdidRetType(NULL),
	m_pstrFunc(NULL),
	m_efs(IMDFunction::EfsSentinel),
	m_efda(IMDFunction::EfdaSentinel),
	m_fReturnsSet(false),
	m_fReturnsNullOnNullInput(false),
	m_fBoolReturnType(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::CScalarFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarFunc::CScalarFunc
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	const CWStringConst *pstrFunc
	)
	:
	CScalar(pmp),
	m_pmdidFunc(pmdidFunc),
	m_pmdidRetType(pmdidRetType),
	m_pstrFunc(pstrFunc),
	m_fReturnsSet(false),
	m_fReturnsNullOnNullInput(false),
	m_fBoolReturnType(false)
{
	GPOS_ASSERT(pmdidFunc->FValid());
	GPOS_ASSERT(pmdidRetType->FValid());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDFunction *pmdfunc = pmda->Pmdfunc(m_pmdidFunc);

	m_efs = pmdfunc->EfsStability();
	m_efda = pmdfunc->EfdaDataAccess();
	m_fReturnsSet = pmdfunc->FReturnsSet();

	m_fReturnsNullOnNullInput = pmdfunc->FStrict();
	m_fBoolReturnType = CMDAccessorUtils::FBoolType(pmda, m_pmdidRetType);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::~CScalarFunc
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarFunc::~CScalarFunc()
{
	CRefCount::SafeRelease(m_pmdidFunc);
	CRefCount::SafeRelease(m_pmdidRetType);
	GPOS_DELETE(m_pstrFunc);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::PstrFunc
//
//	@doc:
//		Function name
//
//---------------------------------------------------------------------------
const CWStringConst *
CScalarFunc::PstrFunc() const
{
	return m_pstrFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::PmdidFunc
//
//	@doc:
//		Func id
//
//---------------------------------------------------------------------------
IMDId *
CScalarFunc::PmdidFunc() const
{
	return m_pmdidFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::EFuncStbl
//
//	@doc:
//		Function stability enum
//
//---------------------------------------------------------------------------
IMDFunction::EFuncStbl
CScalarFunc::EfsGetFunctionStability() const
{
	return m_efs;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		id of function
//
//---------------------------------------------------------------------------
ULONG
CScalarFunc::UlHash() const
{
	return gpos::UlCombineHashes(
					COperator::UlHash(),
					gpos::UlCombineHashes(
						m_pmdidFunc->UlHash(),
						m_pmdidRetType->UlHash()));
}

	
//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarFunc::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
		
	}
	CScalarFunc *popScFunc = CScalarFunc::PopConvert(pop);

	// match if func ids are identical
	return popScFunc->PmdidFunc()->FEquals(m_pmdidFunc) &&
			popScFunc->PmdidType()->FEquals(m_pmdidRetType);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarFunc::PmdidType() const
{
	return m_pmdidRetType;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::FHasNonScalarFunction
//
//	@doc:
//		Derive existence of non-scalar functions from expression handle
//
//---------------------------------------------------------------------------
BOOL
CScalarFunc::FHasNonScalarFunction
	(
	CExpressionHandle & //exprhdl
	)
{
	return m_fReturnsSet;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarFunc::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << PstrFunc()->Wsz();
	os << ")";
	
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarFunc::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarFunc::Eber
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

