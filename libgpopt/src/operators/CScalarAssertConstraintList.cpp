//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2015 Pivotal, Inc.
//
//	@filename:
//		CScalarAssertConstraintList.cpp
//
//	@doc:
//		Implementation of scalar assert constraint list representing the predicate
//		of assert operators
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "naucrates/md/IMDTypeBool.h"
#include "gpopt/operators/CScalarAssertConstraintList.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraintList::CScalarAssertConstraintList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarAssertConstraintList::CScalarAssertConstraintList
	(
	IMemoryPool *pmp
	)
	:
	CScalar(pmp)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraintList::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarAssertConstraintList::FMatch
	(
	COperator *pop
	)
	const
{
	return pop->Eopid() == Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarAssertConstraintList::FMatch
//
//	@doc:
//		Type of expression's result
//
//---------------------------------------------------------------------------
IMDId *
CScalarAssertConstraintList::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


// EOF

