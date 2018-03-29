//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSequence.cpp
//
//	@doc:
//		Implementation of physical sequence operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecNonSingleton.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CCTEReq.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::CPhysicalSequence
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSequence::CPhysicalSequence
	(
	IMemoryPool *pmp
	)
	:
	CPhysical(pmp),
	m_pcrsEmpty(NULL)
{
	// Sequence generates two distribution requests for its children:
	// (1) If incoming distribution from above is Singleton, pass it through
	//		to all children, otherwise request Non-Singleton on all children
	//
	// (2)	Optimize first child with Any distribution requirement, and compute
	//		distribution request on other children based on derived distribution
	//		of first child:
	//			* If distribution of first child is a Singleton, request Singleton
	//				on all children
	//			* If distribution of first child is a Non-Singleton, request
	//				Non-Singleton on all children

	SetDistrRequests(2);

	m_pcrsEmpty = GPOS_NEW(pmp) CColRefSet(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::~CPhysicalSequence
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSequence::~CPhysicalSequence()
{
	m_pcrsEmpty->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSequence::FMatch
	(
	COperator *pop
	)
	const
{
	return Eopid() == pop->Eopid();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PcrsRequired
//
//	@doc:
//		Compute required output columns of n-th child
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalSequence::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	const ULONG ulArity = exprhdl.UlArity();
	if (ulChildIndex == ulArity - 1)
	{
		// request required columns from the last child of the sequence
		return PcrsChildReqd(pmp, exprhdl, pcrsRequired, ulChildIndex,
							 gpos::ulong_max);
	}
	
	m_pcrsEmpty->AddRef();
	GPOS_ASSERT(0 == m_pcrsEmpty->CElements());

	return m_pcrsEmpty;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalSequence::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG ulChildIndex,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(NULL != pppsRequired);
	
	return CPhysical::PppsRequiredPushThruNAry(pmp, exprhdl, pppsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalSequence::PcteRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CCTEReq *pcter,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcter);
	if (ulChildIndex < exprhdl.UlArity() - 1)
	{
		return pcter->PcterAllOptional(pmp);
	}

	// derived CTE maps from previous children
	CCTEMap *pcmCombined = PcmCombine(pmp, pdrgpdpCtxt);

	// pass the remaining requirements that have not been resolved
	CCTEReq *pcterUnresolved = pcter->PcterUnresolvedSequence(pmp, pcmCombined, pdrgpdpCtxt);
	pcmCombined->Release();

	return pcterUnresolved;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::FProvidesReqdCols
//
//	@doc:
//		Helper for checking if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSequence::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);

	// last child must provide required columns
	ULONG ulArity = exprhdl.UlArity();
	GPOS_ASSERT(0 < ulArity);
	
	CColRefSet *pcrsChild = exprhdl.Pdprel(ulArity - 1)->PcrsOutput();

	return pcrsChild->FSubset(pcrsRequired);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSequence::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &
#ifdef GPOS_DEBUG
	exprhdl
#endif // GPOS_DEBUG
	,
	CDistributionSpec *pdsRequired,
	ULONG  ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG  ulOptReq
	)
	const
{
	GPOS_ASSERT(2 == exprhdl.UlArity());
	GPOS_ASSERT(ulChildIndex < exprhdl.UlArity());
	GPOS_ASSERT(ulOptReq < UlDistrRequests());

	if (0 == ulOptReq)
	{
		if (CDistributionSpec::EdtSingleton == pdsRequired->Edt() ||
			CDistributionSpec::EdtStrictSingleton == pdsRequired->Edt())
		{
			// incoming request is a singleton, request singleton on all children
			CDistributionSpecSingleton *pdss = CDistributionSpecSingleton::PdssConvert(pdsRequired);
			return GPOS_NEW(pmp) CDistributionSpecSingleton(pdss->Est());
		}

		// incoming request is a non-singleton, request non-singleton on all children
		return GPOS_NEW(pmp) CDistributionSpecNonSingleton();
	}
	GPOS_ASSERT(1 == ulOptReq);

	if (0 == ulChildIndex)
	{
		// no distribution requirement on first child
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
	}

	// get derived plan properties of first child
	CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[0]);
	CDistributionSpec *pds = pdpplan->Pds();

	if (pds->FSingletonOrStrictSingleton())
	{
		// first child is singleton, request singleton distribution on second child
		CDistributionSpecSingleton *pdss = CDistributionSpecSingleton::PdssConvert(pds);
		return GPOS_NEW(pmp) CDistributionSpecSingleton(pdss->Est());
	}

	if (CDistributionSpec::EdtUniversal == pds->Edt())
	{
		// first child is universal, impose no requirements on second child
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
	}

	// first child is non-singleton, request a non-singleton distribution on second child
	return GPOS_NEW(pmp) CDistributionSpecNonSingleton();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSequence::PosRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl,
	COrderSpec * ,// posRequired,
	ULONG , // ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	// no order requirement on the children
	return GPOS_NEW(pmp) COrderSpec(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PrsRequired
//
//	@doc:
//		Compute required rewindability order of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSequence::PrsRequired
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &, // exprhdl,
	CRewindabilitySpec *, // prsRequired,
	ULONG , // ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	// no rewindability required on the children
	return GPOS_NEW(m_pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSequence::PosDerive
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	// pass through sort order from last child
	const ULONG ulArity = exprhdl.UlArity();
	
	GPOS_ASSERT(1 <= ulArity);
	
	COrderSpec *pos = exprhdl.Pdpplan(ulArity - 1 /*ulChildIndex*/)->Pos();
	pos->AddRef();

	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSequence::PdsDerive
	(
	IMemoryPool *, // pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	// pass through distribution from last child
	const ULONG ulArity = exprhdl.UlArity();
	
	GPOS_ASSERT(1 <= ulArity);
	
	CDistributionSpec *pds = exprhdl.Pdpplan(ulArity - 1 /*ulChildIndex*/)->Pds();
	pds->AddRef();

	return pds;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSequence::PrsDerive
	(
	IMemoryPool *, // pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	// no rewindability by sequence
	return GPOS_NEW(m_pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::EpetOrder
//
//	@doc:
//		Return the enforcing type for the order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSequence::EpetOrder
	(
	CExpressionHandle &exprhdl,
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);

	// get order delivered by the sequence node
	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();

	if (peo->FCompatible(pos))
	{
		// required order will be established by the sequence operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required distribution will be enforced on sequence's output
	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSequence::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSequence::EpetRewindability
	(
	CExpressionHandle &, // exprhdl
	const CEnfdRewindability * // per
	)
	const
{
	// rewindability must be enforced on operator's output
	return CEnfdProp::EpetRequired;
}


// EOF
