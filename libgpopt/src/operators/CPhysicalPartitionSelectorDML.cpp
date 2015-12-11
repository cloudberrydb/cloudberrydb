//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CPhysicalPartitionSelectorDML.cpp
//
//	@doc:
//		Implementation of physical partition selector for DML
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
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRouted.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalPartitionSelectorDML.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::CPhysicalPartitionSelectorDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalPartitionSelectorDML::CPhysicalPartitionSelectorDML
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	HMUlExpr *phmulexprEqPredicates,
	CColRef *pcrOid
	)
	:
	CPhysicalPartitionSelector(pmp, pmdid, phmulexprEqPredicates),
	m_pcrOid(pcrOid)
{
	GPOS_ASSERT(NULL != pcrOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::FMatch
//
//	@doc:
//		Match operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelectorDML::FMatch
	(
	COperator *pop
	)
	const
{
	if (Eopid() != pop->Eopid())
	{
		return false;
	}

	CPhysicalPartitionSelectorDML *popPartSelector = CPhysicalPartitionSelectorDML::PopConvert(pop);

	return popPartSelector->Pmdid()->FEquals(m_pmdid) &&
			popPartSelector->PcrOid() == m_pcrOid &&
			FMatchExprMaps(popPartSelector->m_phmulexprEqPredicates, m_phmulexprEqPredicates);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::UlHash
//
//	@doc:
//		Hash operator
//
//---------------------------------------------------------------------------
ULONG
CPhysicalPartitionSelectorDML::UlHash() const
{
	return gpos::UlCombineHashes(Eopid(), m_pmdid->UlHash());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::PpfmDerive
//
//	@doc:
//		Derive partition filter map
//
//---------------------------------------------------------------------------
CPartFilterMap *
CPhysicalPartitionSelectorDML::PpfmDerive
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	return PpfmPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalPartitionSelectorDML::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CDistributionSpec *pdsInput,
	ULONG ulChildIndex,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	// if required distribution uses any defined column, it has to be enforced on top,
	// in this case, we request Any distribution from the child
	CColRefSet *pcrs = NULL;
	CDistributionSpec::EDistributionType edtRequired = pdsInput->Edt();
	if (CDistributionSpec::EdtHashed == edtRequired)
	{
		CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pdsInput);
		pcrs = pdshashed->PcrsUsed(m_pmp);
	}

	if (CDistributionSpec::EdtRouted == edtRequired)
	{
		CDistributionSpecRouted *pdsrouted = CDistributionSpecRouted::PdsConvert(pdsInput);
		pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
		pcrs->Include(pdsrouted->Pcr());
	}

	BOOL fUsesDefinedCols = (NULL != pcrs && pcrs->FMember(m_pcrOid));
	CRefCount::SafeRelease(pcrs);
	if (fUsesDefinedCols)
	{
		return GPOS_NEW(pmp) CDistributionSpecAny();
	}

	return PdsPassThru(pmp, exprhdl, pdsInput, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::PosRequired
//
//	@doc:
//		Compute required sort order of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalPartitionSelectorDML::PosRequired
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
	BOOL fUsesDefinedCols = pcrsSort->FMember(m_pcrOid);
	pcrsSort->Release();

	if (fUsesDefinedCols)
	{
		// if required order uses any column defined here, we cannot
		// request it from child, and we pass an empty order spec;
		// order enforcer function takes care of enforcing this order on top of
		// this operator
		return GPOS_NEW(pmp) COrderSpec(pmp);
	}

	// otherwise, we pass through required order
	return PosPassThru(pmp, exprhdl, posRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalPartitionSelectorDML::FProvidesReqdCols
	(
	CExpressionHandle &exprhdl,
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);
	GPOS_ASSERT(1 == exprhdl.UlArity());

	CColRefSet *pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	// include the defined oid column
	pcrs->Include(m_pcrOid);

	// include output columns of the relational child
	pcrs->Union(exprhdl.Pdprel(0 /*ulChildIndex*/)->PcrsOutput());

	BOOL fProvidesCols = pcrs->FSubset(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalPartitionSelectorDML::PppsRequired
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
//		CPhysicalPartitionSelectorDML::PpimDerive
//
//	@doc:
//		Derive partition index map
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalPartitionSelectorDML::PpimDerive
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &exprhdl,
	CDrvdPropCtxt * //pdpctxt
	)
	const
{
	return PpimPassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelectorDML::EpetOrder
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

	// Sort has to go above if sort columns use any column
	// defined here, otherwise, Sort can either go above or below
	CColRefSet *pcrsSort = peo->PosRequired()->PcrsUsed(m_pmp);
	BOOL fUsesDefinedCols = pcrsSort->FMember(m_pcrOid);
	pcrsSort->Release();
	if (fUsesDefinedCols)
	{
		return CEnfdProp::EpetRequired;
	}

	return CEnfdProp::EpetOptional;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalPartitionSelectorDML::EpetDistribution
	(
	CExpressionHandle &exprhdl,
	const CEnfdDistribution *ped
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	// get distribution delivered by the filter node
	CDistributionSpec *pds = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pds();
	if (ped->FCompatible(pds))
	{
		 // required distribution is already provided
		 return CEnfdProp::EpetUnnecessary;
	}

	if (exprhdl.FHasOuterRefs())
	{
		return CEnfdProp::EpetProhibited;
	}

	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalPartitionSelectorDML::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalPartitionSelectorDML::OsPrint
	(
	IOstream &os
	)
	const
{

	os	<< SzId()
		<< ", Part Table: ";
	m_pmdid->OsPrint(os);

	return os;
}

// EOF
