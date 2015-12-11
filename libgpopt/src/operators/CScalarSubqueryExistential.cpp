//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CScalarSubqueryExistential.cpp
//
//	@doc:
//		Implementation of existential subquery operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDrvdPropScalar.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CScalarSubqueryExistential.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::CScalarSubqueryExistential
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryExistential::CScalarSubqueryExistential
	(
	IMemoryPool *pmp
	)
	:
	CScalar(pmp)
{}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::~CScalarSubqueryExistential
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CScalarSubqueryExistential::~CScalarSubqueryExistential()
{}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::PmdidType
//
//	@doc:
//		Type of scalar's value
//
//---------------------------------------------------------------------------
IMDId *
CScalarSubqueryExistential::PmdidType() const
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	return pmda->PtMDType<IMDTypeBool>()->Pmdid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::FMatch
//
//	@doc:
//		Match function on operator level
//
//---------------------------------------------------------------------------
BOOL
CScalarSubqueryExistential::FMatch
	(
	COperator *pop
	)
	const
{
	GPOS_ASSERT(NULL != pop);

	return pop->Eopid() == Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryExistential::PpartinfoDerive
//
//	@doc:
//		Derive partition consumers
//
//---------------------------------------------------------------------------
CPartInfo *
CScalarSubqueryExistential::PpartinfoDerive
	(
	IMemoryPool *, // pmp, 
	CExpressionHandle &exprhdl
	)
	const
{
	CPartInfo *ppartinfoChild = exprhdl.Pdprel(0 /*ulChildIndex*/)->Ppartinfo();
	GPOS_ASSERT(NULL != ppartinfoChild);
	ppartinfoChild->AddRef();
	return ppartinfoChild;
}

// EOF

