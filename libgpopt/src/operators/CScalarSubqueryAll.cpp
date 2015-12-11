//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CScalarSubqueryAll.cpp
//
//	@doc:
//		Implementation of scalar subquery ALL operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/operators/CScalarSubqueryAll.h"
#include "gpopt/base/CUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CScalarSubqueryAll::CScalarSubqueryAll
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CScalarSubqueryAll::CScalarSubqueryAll
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
//		CScalarSubqueryAll::PopCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the operator with remapped columns
//
//---------------------------------------------------------------------------
COperator *
CScalarSubqueryAll::PopCopyWithRemappedColumns
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

	return GPOS_NEW(pmp) CScalarSubqueryAll(pmp, pmdidScalarOp, pstrScalarOp, pcr);
}

// EOF
