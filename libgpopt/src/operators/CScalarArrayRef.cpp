//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CScalarArrayRef.cpp
//
//	@doc:
//		Implementation of scalar arrayref
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/CScalarArrayRef.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayRef::CScalarArrayRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarArrayRef::CScalarArrayRef
	(
	IMemoryPool *pmp,
	IMDId *pmdidElem,
	INT iTypeModifier,
	IMDId *pmdidArray,
	IMDId *pmdidReturn
	)
	:
	CScalar(pmp),
	m_pmdidElem(pmdidElem),
	m_iTypeModifier(iTypeModifier),
	m_pmdidArray(pmdidArray),
	m_pmdidType(pmdidReturn)
{
	GPOS_ASSERT(pmdidElem->FValid());
	GPOS_ASSERT(pmdidArray->FValid());
	GPOS_ASSERT(pmdidReturn->FValid());
	GPOS_ASSERT(pmdidReturn->FEquals(pmdidElem) || pmdidReturn->FEquals(pmdidArray));
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayRef::~CScalarArrayRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarArrayRef::~CScalarArrayRef()
{
	m_pmdidElem->Release();
	m_pmdidArray->Release();
	m_pmdidType->Release();
}


INT
CScalarArrayRef::ITypeModifier() const
{
	return m_iTypeModifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayRef::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarArrayRef::UlHash() const
{
	return gpos::UlCombineHashes
					(
					UlCombineHashes(m_pmdidElem->UlHash(), m_pmdidArray->UlHash()),
					m_pmdidType->UlHash()
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArrayRef::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArrayRef::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}

	CScalarArrayRef *popArrayRef = CScalarArrayRef::PopConvert(pop);

	return m_pmdidType->FEquals(popArrayRef->PmdidType()) &&
			m_pmdidElem->FEquals(popArrayRef->PmdidElem()) &&
			m_pmdidArray->FEquals(popArrayRef->PmdidArray());
}

// EOF

