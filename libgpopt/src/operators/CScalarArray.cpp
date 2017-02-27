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

// Ctor
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
	m_pdrgPconst = GPOS_NEW(pmp) DrgPconst(pmp);
}


// Ctor
CScalarArray::CScalarArray
	(
	IMemoryPool *pmp,
	IMDId *pmdidElem,
	IMDId *pmdidArray,
	BOOL fMultiDimensional,
	DrgPconst *pdrgPconst
	)
:
CScalar(pmp),
m_pmdidElem(pmdidElem),
m_pmdidArray(pmdidArray),
m_fMultiDimensional(fMultiDimensional),
m_pdrgPconst(pdrgPconst)
{
	GPOS_ASSERT(pmdidElem->FValid());
	GPOS_ASSERT(pmdidArray->FValid());
}

// Dtor
CScalarArray::~CScalarArray()
{
	m_pmdidElem->Release();
	m_pmdidArray->Release();
	m_pdrgPconst->Release();
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
		if (popArray->FMultiDimensional() == FMultiDimensional() &&
			PmdidElem()->FEquals(popArray->PmdidElem()) &&
			PmdidArray()->FEquals(popArray->PmdidArray()) &&
			m_pdrgPconst->UlLength() == popArray->PdrgPconst()->UlLength())
		{
			for (ULONG ul = 0; ul < m_pdrgPconst->UlLength(); ul++)
			{
				CScalarConst *popConst1 = (*m_pdrgPconst)[ul];
				CScalarConst *popConst2 = (*popArray->PdrgPconst())[ul];
				if (!popConst1->FMatch(popConst2))
				{
					return false;
				}
			}
			return true;
		}
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

DrgPconst *
CScalarArray::PdrgPconst() const
{
	return m_pdrgPconst;
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
	for (ULONG ul = 0; ul < m_pdrgPconst->UlLength(); ul++)
	{
		os << " ";
		(*m_pdrgPconst)[ul]->OsPrint(os);
	}
	os << "}";
	return os;
}


// EOF

