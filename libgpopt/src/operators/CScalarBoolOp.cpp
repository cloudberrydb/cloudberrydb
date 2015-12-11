//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarBoolOp.cpp
//
//	@doc:
//		Implementation of scalar boolean operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"

#include "gpopt/operators/CScalarBoolOp.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;

const WCHAR CScalarBoolOp::m_rgwszBool[EboolopSentinel][30] =
{
	GPOS_WSZ_LIT("EboolopAnd"),
	GPOS_WSZ_LIT("EboolopOr"),
	GPOS_WSZ_LIT("EboolopNot")
};

//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::UlHash
//
//	@doc:
//		Operator specific hash function; combined hash of operator id and
//		id of comparison
//
//---------------------------------------------------------------------------
ULONG
CScalarBoolOp::UlHash() const
{
	ULONG ulBoolop = (ULONG) Eboolop();
	return gpos::UlCombineHashes(COperator::UlHash(),
							    gpos::UlHash<ULONG>(&ulBoolop));
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarBoolOp::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CScalarBoolOp *popLog = CScalarBoolOp::PopConvert(pop);

		// match if operators are identical
		return Eboolop() == popLog->Eboolop();
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::FCommutative
//
//	@doc:
//		Is boolean operator commutative?
//
//---------------------------------------------------------------------------
BOOL
CScalarBoolOp::FCommutative
	(
	EBoolOperator eboolop
	)
{
	switch(eboolop)
	{
		case EboolopAnd:
		case EboolopOr:
			return true;

		case EboolopNot:

		default:
			return false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarBoolOp::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarBoolOp::Eber
	(
	DrgPul *pdrgpulChildren
	)
	const
{
	if (EboolopAnd == m_eboolop)
	{
		return EberConjunction(pdrgpulChildren);
	}

	if (EboolopOr == m_eboolop)
	{
		return EberDisjunction(pdrgpulChildren);
	}

	GPOS_ASSERT(EboolopNot == m_eboolop);
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 == pdrgpulChildren->UlLength());

	EBoolEvalResult eber = (EBoolEvalResult) *((*pdrgpulChildren)[0]);
	switch (eber)
	{
		case EberTrue:
			return EberFalse;

		case EberFalse:
			return EberTrue;

		case EberNull:
			return EberNull;

		default:
			return EberUnknown;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarBoolOp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarBoolOp::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (";
	os << m_rgwszBool[m_eboolop];
	os << ")";

	return os;
}


// EOF

