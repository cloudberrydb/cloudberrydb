//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarArray.cpp
//
//	@doc:
//		Implementation of scalar arrays
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDAggregate.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"

#include "gpopt/operators/CScalarArray.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::CScalarArray
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarArray::CScalarArray
	(
	IMemoryPool *pmp, 
	IMDId *pmdidElem, 
	IMDId *pmdidArray, 
	BOOL fMultiDimensional
	)
	:
	CScalar(pmp),
	m_pmdidElem(pmdidElem),
	m_pmdidArray(pmdidArray),
	m_fMultiDimensional(fMultiDimensional)
{
	GPOS_ASSERT(pmdidElem->FValid());
	GPOS_ASSERT(pmdidArray->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::~CScalarArray
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarArray::~CScalarArray()
{
	m_pmdidElem->Release();
	m_pmdidArray->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::PmdidElem
//
//	@doc:
//		Element type id
//
//---------------------------------------------------------------------------
IMDId *
CScalarArray::PmdidElem() const
{
	return m_pmdidElem;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::PmdidArray
//
//	@doc:
//		Array type id
//
//---------------------------------------------------------------------------
IMDId *
CScalarArray::PmdidArray() const
{
	return m_pmdidArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::FMultiDimensional
//
//	@doc:
//		Is array multi-dimensional 
//
//---------------------------------------------------------------------------
BOOL
CScalarArray::FMultiDimensional() const
{
	return m_fMultiDimensional;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CScalarArray::UlHash() const
{
	return gpos::UlCombineHashes
					(
					UlCombineHashes(m_pmdidElem->UlHash(), m_pmdidArray->UlHash()),
					gpos::UlHash<BOOL>(&m_fMultiDimensional)
					);
}

	
//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarArray::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarArray *popArray = CScalarArray::PopConvert(pop);
		
		// match if components are identical
		return popArray->FMultiDimensional() ==  m_fMultiDimensional && 
				m_pmdidElem->FEquals(popArray->PmdidElem()) &&
				m_pmdidArray->FEquals(popArray->PmdidArray());
	}
	
	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarArray::PmdidType
//
//	@doc:
//		Type of expression's result
//
//---------------------------------------------------------------------------
IMDId *
CScalarArray::PmdidType() const
{
	return m_pmdidArray;
}

IOstream &
CScalarArray::OsPrint(IOstream &os) const
{
	os << "CScalarArray: {eleMDId: ";
	m_pmdidElem->OsPrint(os);
	os << ", arrayMDId: ";
	m_pmdidArray->OsPrint(os);
	if (m_fMultiDimensional)
	{
		os << ", multidimensional";
	}
	os << "}";
	return os;
}


// EOF

