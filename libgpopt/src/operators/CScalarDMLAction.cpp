//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarDMLAction.cpp
//
//	@doc:
//		Implementation of scalar DML action operator
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

#include "gpopt/operators/CScalarDMLAction.h"
#include "gpopt/operators/CExpressionHandle.h"

#include "naucrates/md/IMDTypeInt4.h"

using namespace gpopt;
using namespace gpmd;


//---------------------------------------------------------------------------
//	@function:
//		CScalarDMLAction::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarDMLAction::FMatch
	(
	COperator *pop
	)
	const
{
	return pop->Eopid() == Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CScalarDMLAction::PmdidType
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
IMDId *
CScalarDMLAction::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeInt4>()->Pmdid();
}
// EOF

