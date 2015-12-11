//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarNullTest.cpp
//
//	@doc:
//		Implementation of scalar null test operator
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

#include "gpopt/operators/CScalarNullTest.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeBool.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarNullTest::FMatch
	(
	COperator *pop
	)
	const
{
	return pop->Eopid() == Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::PmdidType
//
//	@doc:
//		Expression type
//
//---------------------------------------------------------------------------
IMDId *
CScalarNullTest::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarNullTest::Eber
//
//	@doc:
//		Perform boolean expression evaluation
//
//---------------------------------------------------------------------------
CScalar::EBoolEvalResult
CScalarNullTest::Eber
	(
	DrgPul *pdrgpulChildren
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpulChildren);
	GPOS_ASSERT(1 == pdrgpulChildren->UlLength());

	EBoolEvalResult eber = (EBoolEvalResult) *((*pdrgpulChildren)[0]);
	switch (eber)
	{
		case EberNull:
			return EberTrue;

		case EberFalse:
		case EberTrue:
			return EberFalse;

		default:
			return EberUnknown;
	}
}


// EOF

