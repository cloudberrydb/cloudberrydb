//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalMotionBroadcast.cpp
//
//	@doc:
//		Implementation of broadcast motion operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalMotionBroadcast.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::CPhysicalMotionBroadcast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast::CPhysicalMotionBroadcast
	(
	IMemoryPool *pmp
	)
	:
	CPhysicalMotion(pmp),
	m_pdsReplicated(NULL)
{
	m_pdsReplicated = GPOS_NEW(pmp) CDistributionSpecReplicated();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::~CPhysicalMotionBroadcast
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast::~CPhysicalMotionBroadcast()
{
	m_pdsReplicated->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcast::FMatch
	(
	COperator *pop
	)
	const
{
	return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalMotionBroadcast::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, *pcrsRequired);

	CColRefSet *pcrsChildReqd =
		PcrsChildReqd(pmp, exprhdl, pcrs, ulChildIndex, gpos::ulong_max);
	pcrs->Release();

	return pcrsChildReqd;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalMotionBroadcast::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	return FUnaryProvidesReqdCols(exprhdl, pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalMotionBroadcast::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder * // peo
	)
	const
{
	// broadcast motion is not order-preserving
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionBroadcast::PosRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	COrderSpec *,//posInput
	ULONG 
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// no order required from child expression
	return GPOS_NEW(pmp) COrderSpec(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalMotionBroadcast::PosDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	// broadcast motion is not order-preserving
	return GPOS_NEW(pmp) COrderSpec(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalMotionBroadcast::OsPrint
	(
	IOstream &os
	)
	const
{
	os << SzId() << " ";
	return os;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalMotionBroadcast::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalMotionBroadcast *
CPhysicalMotionBroadcast::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(EopPhysicalMotionBroadcast == pop->Eopid());
	
	return dynamic_cast<CPhysicalMotionBroadcast*>(pop);
}			

// EOF

