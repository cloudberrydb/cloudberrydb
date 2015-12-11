//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalLeftAntiSemiNLJoin.cpp
//
//	@doc:
//		Implementation of left anti semi nested-loops join operator
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

#include "gpopt/operators/CPhysicalLeftAntiSemiNLJoin.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiNLJoin::CPhysicalLeftAntiSemiNLJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftAntiSemiNLJoin::CPhysicalLeftAntiSemiNLJoin
	(
	IMemoryPool *pmp
	)
	:
	CPhysicalNLJoin(pmp)
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiNLJoin::~CPhysicalLeftAntiSemiNLJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLeftAntiSemiNLJoin::~CPhysicalLeftAntiSemiNLJoin()
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiNLJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalLeftAntiSemiNLJoin::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	// left anti semi join only propagates columns from left child
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftAntiSemiNLJoin::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalLeftAntiSemiNLJoin::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt,
	ULONG // ulOptReq
	)
{
	// no partition elimination for LASJ: push request to the respective child
	return CPhysical::PppsRequiredPushThruNAry(pmp, exprhdl, pppsRequired, ulChildIndex);
}

// EOF

