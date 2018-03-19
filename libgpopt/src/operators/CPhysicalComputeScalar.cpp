//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalComputeScalar.cpp
//
//	@doc:
//		Implementation of ComputeScalar operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecStrictSingleton.h"
#include "gpopt/base/CDistributionSpecRouted.h"

#include "gpopt/operators/CPhysicalComputeScalar.h"
#include "gpopt/operators/CExpressionHandle.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::CPhysicalComputeScalar
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalComputeScalar::CPhysicalComputeScalar
	(
	IMemoryPool *pmp
	)
	:
	CPhysical(pmp)
{
	// when ComputeScalar includes no outer references, we create two optimization requests
	// to enforce distribution of its child:
	// (1) Any: impose no distribution requirement on the child in order to push scalar computation below
	// Motions, and then enforce required distribution on top of ComputeScalar if needed
	// (2) Pass-Thru: impose distribution requirement on child, and then perform scalar computation after
	// Motions are enforced, this is more efficient for Master-Only plans below ComputeScalar

	// when ComputeScalar includes outer references, correlated execution has to be enforced,
	// in this case, we create two child optimization requests to guarantee correct evaluation of parameters
	// (1) Broadcast
	// (2) Singleton

	SetDistrRequests(2 /*ulDistrReqs*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::~CPhysicalComputeScalar
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalComputeScalar::~CPhysicalComputeScalar()
{}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalComputeScalar::FMatch
	(
	COperator *pop
	)
	const
{
	// ComputeScalar doesn't contain any members as of now
	return Eopid() == pop->Eopid();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalComputeScalar::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex &&
				"Required properties can only be computed on the relational child");
	
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, *pcrsRequired);
	CColRefSet *pcrsChildReqd = PcrsChildReqd(pmp, exprhdl, pcrs, ulChildIndex, 1 /*ulScalarIndex*/);
	pcrs->Release();
	
	return pcrsChildReqd;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalComputeScalar::PosRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	COrderSpec *posRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	CColRefSet *pcrsSort = posRequired->PcrsUsed(m_pmp);
	BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrsSort, exprhdl);
	pcrsSort->Release();

	if (fUsesDefinedCols)
	{
		// if required order uses any column defined by ComputeScalar, we cannot
		// request it from child, and we pass an empty order spec;
		// order enforcer function takes care of enforcing this order on top of
		// ComputeScalar operator
		return GPOS_NEW(pmp) COrderSpec(pmp);
	}

	// otherwise, we pass through required order
	return PosPassThru(pmp, exprhdl, posRequired, ulChildIndex);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalComputeScalar::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(2 > ulOptReq);

	// check if master-only/replicated distribution needs to be requested
	CDistributionSpec *pds = PdsMasterOnlyOrReplicated(pmp, exprhdl, pdsRequired, ulChildIndex, ulOptReq);
	if (NULL != pds)
	{
		return pds;
	}

	CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(1 /*ulChildIndex*/);

	// if a Project operator has a call to a set function, passing a Random distribution through this
	// Project may have the effect of not distributing the results of the set function to all nodes,
	// but only to the nodes on which first child of the Project is distributed.
	// to avoid that, we don't push the distribution requirement in this case and thus, for a random
	// distribution, the result of the set function is spread uniformly over all nodes
	if (pdpscalar->FHasNonScalarFunction())
	{
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
	}

	// if required distribution uses any defined column, it has to be enforced on top of ComputeScalar,
	// in this case, we request Any distribution from the child
	CDistributionSpec::EDistributionType edtRequired = pdsRequired->Edt();
	if (CDistributionSpec::EdtHashed == edtRequired)
	{
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pdsRequired);
		CColRefSet *pcrs = pdshashed->PcrsUsed(m_pmp);
		BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrs, exprhdl);
		pcrs->Release();
		if (fUsesDefinedCols)
		{
			return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
		}
	}

	if (CDistributionSpec::EdtRouted == edtRequired)
	{
		CDistributionSpecRouted *pdsrouted = CDistributionSpecRouted::PdsConvert(pdsRequired);
		CColRefSet *pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
		pcrs->Include(pdsrouted->Pcr());
		BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrs, exprhdl);
		pcrs->Release();
		if (fUsesDefinedCols)
		{
			return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
		}
	}

	if (0 == ulOptReq)
	{
		// Req0: required distribution will be enforced on top of ComputeScalar
		return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
	}

	// Req1: required distribution will be enforced on top of ComputeScalar's child
	return PdsPassThru(pmp, exprhdl, pdsRequired, ulChildIndex);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalComputeScalar::PrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CRewindabilitySpec *prsRequired,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// if there are outer references, then we need a materialize
	if (exprhdl.FHasOuterRefs())
	{
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtGeneral);
	}

	return PrsPassThru(pmp, exprhdl, prsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalComputeScalar::PppsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CPartitionPropagationSpec *pppsRequired,
	ULONG ulChildIndex,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);
	GPOS_ASSERT(NULL != pppsRequired);
	
	return CPhysical::PppsRequiredPushThru(pmp, exprhdl, pppsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalComputeScalar::PcteRequired
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &, //exprhdl,
	CCTEReq *pcter,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif
	,
	DrgPdp *, //pdrgpdpCtxt,
	ULONG //ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);
	return PcterPushThru(pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalComputeScalar::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(2 == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	// include defined columns by scalar project list
	pcrs->Union(exprhdl.Pdpscalar(1 /*ulChildIndex*/)->PcrsDefined());

	// include output columns of the relational child
	pcrs->Union(exprhdl.Pdprel(0 /*ulChildIndex*/)->PcrsOutput());

	BOOL fProvidesCols = pcrs->FSubset(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalComputeScalar::PosDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PosDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalComputeScalar::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDistributionSpec *pds = exprhdl.Pdpplan(0 /*ulChildIndex*/)->Pds();
	
	if (CDistributionSpec::EdtUniversal == pds->Edt() && 
		IMDFunction::EfsVolatile == exprhdl.Pdpscalar(1 /*ulChildIndex*/)->Pfp()->Efs())
	{
		return GPOS_NEW(pmp) CDistributionSpecStrictSingleton(CDistributionSpecSingleton::EstMaster);
	}
	
	pds->AddRef();

	return pds;
	
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalComputeScalar::PrsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDrvdPropScalar *pdpscalar = exprhdl.Pdpscalar(1 /*ulChildIndex*/);
	if (pdpscalar->FHasNonScalarFunction() || IMDFunction::EfsVolatile == pdpscalar->Pfp()->Efs())
	{
		// ComputeScalar is not rewindable if it has non-scalar/volatile functions in project list
		return GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	}

	return PrsDerivePassThruOuter(exprhdl);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalComputeScalar::EpetOrder
	(
	CExpressionHandle &exprhdl,
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());
	
	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		return  CEnfdProp::EpetUnnecessary;
	}

	// Sort has to go above ComputeScalar if sort columns use any column
	// defined by ComputeScalar, otherwise, Sort can either go above or below ComputeScalar
	CColRefSet *pcrsSort = peo->PosRequired()->PcrsUsed(m_pmp);
	BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrsSort, exprhdl);
	pcrsSort->Release();
	if (fUsesDefinedCols)
	{
		return CEnfdProp::EpetRequired;
	}

	return CEnfdProp::EpetOptional;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalComputeScalar::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalComputeScalar::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	CColRefSet *pcrsUsed = exprhdl.Pdpscalar(1 /*ulChidIndex*/)->PcrsUsed();
	CColRefSet *pcrsCorrelatedApply = exprhdl.Pdprel()->PcrsCorrelatedApply();
	if (!pcrsUsed->FDisjoint(pcrsCorrelatedApply))
	{
		// columns are used from inner children of correlated-apply expressions,
		// this means that a subplan occurs below the Project operator,
		// in this case, rewindability needs to be enforced on operator's output

		return CEnfdProp::EpetRequired;
	}

	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		 // required distribution is already provided
		 return CEnfdProp::EpetUnnecessary;
	}

	// rewindability is enforced on operator's output
	return CEnfdProp::EpetRequired;
}

// EOF

