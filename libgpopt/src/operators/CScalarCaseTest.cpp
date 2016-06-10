//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarCaseTest.cpp
//
//	@doc:
//		Implementation of scalar case test operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CScalarCaseTest.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::CScalarCaseTest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarCaseTest::CScalarCaseTest
	(
	IMemoryPool *pmp,
	IMDId *pmdidType
	)
	:
	CScalar(pmp),
	m_pmdidType(pmdidType)
{
	GPOS_ASSERT(pmdidType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::~CScalarCaseTest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarCaseTest::~CScalarCaseTest()
{
	m_pmdidType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		return type id
//
//---------------------------------------------------------------------------
ULONG
CScalarCaseTest::UlHash() const
{
	return gpos::UlCombineHashes(COperator::UlHash(), m_pmdidType->UlHash());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CScalarCaseTest::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected call of function FInputOrderSensitive");
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarCaseTest::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarCaseTest::FMatch
	(
	COperator *pop
	)
	const
{
	if(pop->Eopid() == Eopid())
	{
		CScalarCaseTest *popScCaseTest = CScalarCaseTest::PopConvert(pop);

		// match if return types are identical
		return popScCaseTest->PmdidType()->FEquals(m_pmdidType);
	}

	return false;
}

// EOF

