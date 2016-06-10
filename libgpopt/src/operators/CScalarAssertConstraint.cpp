//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CScalarAssertConstraint.cpp
//
//	@doc:
//		Implementation of scalar assert constraint
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDTypeBool.h"
#include "gpopt/operators/CScalarAssertConstraint.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraint::CScalarAssertConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarAssertConstraint::CScalarAssertConstraint
	(
	IMemoryPool *pmp,
	CWStringBase *pstrErrorMsg
	)
	:
	CScalar(pmp),
	m_pstrErrorMsg(pstrErrorMsg)
{
	GPOS_ASSERT(NULL != pstrErrorMsg);
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraint::CScalarAssertConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarAssertConstraint::~CScalarAssertConstraint()
{
	GPOS_DELETE(m_pstrErrorMsg);
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraint::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarAssertConstraint::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() != Eopid())
	{
		return false;
	}
	
	return m_pstrErrorMsg->FEquals(CScalarAssertConstraint::PopConvert(pop)->PstrErrorMsg());
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraint::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CScalarAssertConstraint::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " (ErrorMsg: ";
	os << PstrErrorMsg()->Wsz();
	os << ")";
	
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraint::PmdidType
//
//	@doc:
//		Type of expression's result
//
//---------------------------------------------------------------------------
IMDId *
CScalarAssertConstraint::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


// EOF

