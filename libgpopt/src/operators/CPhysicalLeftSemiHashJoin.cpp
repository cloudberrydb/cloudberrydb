//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalLeftSemiHashJoin.cpp
//
//	@doc:
//		Implementation of left semi hash join operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/operators/CPhysicalLeftSemiHashJoin.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalLeftSemiHashJoin::CPhysicalLeftSemiHashJoin
	(
	IMemoryPool *mp,
	CExpressionArray *pdrgpexprOuterKeys,
	CExpressionArray *pdrgpexprInnerKeys
	)
	:
	CPhysicalHashJoin(mp, pdrgpexprOuterKeys, pdrgpexprInnerKeys)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::~CPhysicalLeftSemiHashJoin
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalLeftSemiHashJoin::~CPhysicalLeftSemiHashJoin()
{
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalLeftSemiHashJoin::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	// left semi join only propagates columns from left child
	return FOuterProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalLeftSemiHashJoin::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalLeftSemiHashJoin::PppsRequired
	(
	IMemoryPool *mp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG child_index,
	CDrvdProp2dArray *pdrgpdpCtxt,
	ULONG // ulOptReq
	)
{
	return PppsRequiredJoinChild(mp, exprhdl, pppsRequired, child_index, pdrgpdpCtxt, false);
}

// EOF

