//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSubqueryAny.cpp
//
//	@doc:
//		Implementation of scalar subquery ANY operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarSubqueryAny.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAny::CScalarSubqueryAny
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryAny::CScalarSubqueryAny
	(
	IMemoryPool *pmp,
	IMDId *pmdidScalarOp,
	const CWStringConst *pstrScalarOp,
	const CColRef *pcr
	)
	:
	CScalarSubqueryQuantified(pmp, pmdidScalarOp, pstrScalarOp, pcr)
{}

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAny::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarSubqueryAny::PopCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CColRef *pcr = CUtils::PcrRemap(Pcr(), phmulcr, fMustExist);

	IMDId *pmdidScalarOp = PmdidOp();
	pmdidScalarOp->AddRef();

	CWStringConst *pstrScalarOp = GPOS_NEW(pmp) CWStringConst(pmp, PstrOp()->Wsz());

	return GPOS_NEW(pmp) CScalarSubqueryAny(pmp, pmdidScalarOp, pstrScalarOp, pcr);
}

// EOF
