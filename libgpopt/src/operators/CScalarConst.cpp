//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarConst.cpp
//
//	@doc:
//		Implementation of scalar constant operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "naucrates/base/IDatumBool.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CExpressionHandle.h"

using namespace gpopt;
using namespace gpnaucrates;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::CScalarConst
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarConst::CScalarConst
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	:
	CScalar(pmp),
	m_pdatum(pdatum)
{
	GPOS_ASSERT(NULL != pdatum);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::~CScalarConst
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarConst::~CScalarConst()
{
	m_pdatum->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		hash of constant value
//
//---------------------------------------------------------------------------
ULONG
CScalarConst::UlHash() const
{
	return gpos::UlCombineHashes
			(
			COperator::UlHash(),
			m_pdatum->UlHash()
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarConst::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarConst *psconst = CScalarConst::PopConvert(pop);

		// match if constant values are the same
		return Pdatum()->FMatch(psconst->Pdatum());
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarConst::PmdidType() const
{
	return m_pdatum->Pmdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarConst::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	m_pdatum->OsPrint(os);
	os << ")";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::FCastedConst
//
//	@doc:
// 		Is the given expression a cast of a constant expression
//
//---------------------------------------------------------------------------
BOOL
CScalarConst::FCastedConst
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	// cast(constant)
	if (COperator::EopScalarCast == pexpr->Pop()->Eopid())
	{
		if (COperator::EopScalarConst == (*pexpr)[0]->Pop()->Eopid())
		{
			return true;
		}
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::PopExtractFromConstOrCastConst
//
//	@doc:
// 		Extract the constant from the given constant or a casted constant.
// 		Else return NULL.
//
//---------------------------------------------------------------------------
CScalarConst *
CScalarConst::PopExtractFromConstOrCastConst
	(
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	BOOL fScConst = COperator::EopScalarConst == pexpr->Pop()->Eopid();
	BOOL fCastedScConst = CScalarConst::FCastedConst(pexpr);

	// constant or cast(constant)
	if (!fScConst && !fCastedScConst)
	{
		return NULL;
	}

	if (fScConst)
	{
		return CScalarConst::PopConvert(pexpr->Pop());
	}

	GPOS_ASSERT(fCastedScConst);

	return CScalarConst::PopConvert((*pexpr)[0]->Pop());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarConst::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarConst::Eber
	(
	DrgPul * //pdrgpulChildren
	)
	const
{
	if (m_pdatum->FNull())
	{
		return EberNull;
	}

	if (IMDType::EtiBool == m_pdatum->Eti())
	{
		IDatumBool *pdatumBool = dynamic_cast<IDatumBool *>(m_pdatum);
		if (pdatumBool->FValue())
		{
			return EberTrue;
		}

		return EberFalse;
	}

	return EberUnknown;
}

INT
CScalarConst::ITypeModifier() const
{
	return m_pdatum->ITypeModifier();
}


// EOF

