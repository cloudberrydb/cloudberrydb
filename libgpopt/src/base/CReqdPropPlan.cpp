//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CReqdPropPlan.cpp
//
//	@doc:
//		Required plan properties;
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CPrintablePointer.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CEnfdOrder.h"
#include "gpopt/base/CEnfdDistribution.h"
#include "gpopt/base/CEnfdRewindability.h"
#include "gpopt/base/CEnfdPartitionPropagation.h"
#include "gpopt/base/CPartFilterMap.h"
#include "gpopt/base/CReqdPropPlan.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogical.h"
#include "gpopt/operators/CPhysical.h"
#include "gpopt/search/CGroupExpression.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CPartitionPropagationSpec.h"
#include "gpopt/base/CCTEReq.h"
#include "gpopt/base/CPartInfo.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::CReqdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan
	(
	CColRefSet *pcrs,
	CEnfdOrder *peo,
	CEnfdDistribution *ped,
	CEnfdRewindability *per,
	CCTEReq *pcter
	)
	:
	m_pcrs(pcrs),
	m_peo(peo),
	m_ped(ped),
	m_per(per),
	m_pepp(NULL),
	m_pcter(pcter)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(NULL != ped);
	GPOS_ASSERT(NULL != per);
	GPOS_ASSERT(NULL != pcter);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::CReqdPropPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CReqdPropPlan::CReqdPropPlan
	(
	CColRefSet *pcrs,
	CEnfdOrder *peo,
	CEnfdDistribution *ped,
	CEnfdRewindability *per,
	CEnfdPartitionPropagation *pepp,
	CCTEReq *pcter
	)
	:
	m_pcrs(pcrs),
	m_peo(peo),
	m_ped(ped),
	m_per(per),
	m_pepp(pepp),
	m_pcter(pcter)
{
	GPOS_ASSERT(NULL != pcrs);
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(NULL != ped);
	GPOS_ASSERT(NULL != per);
	GPOS_ASSERT(NULL != pepp);
	GPOS_ASSERT(NULL != pcter);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::~CReqdPropPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CReqdPropPlan::~CReqdPropPlan()
{
	CRefCount::SafeRelease(m_pcrs);
	CRefCount::SafeRelease(m_peo);
	CRefCount::SafeRelease(m_ped);
	CRefCount::SafeRelease(m_per);
	CRefCount::SafeRelease(m_pepp);
	CRefCount::SafeRelease(m_pcter);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCols
//
//	@doc:
//		Compute required columns
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::ComputeReqdCols
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdProp *prpInput,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt
	)
{
	GPOS_ASSERT(NULL == m_pcrs);

	CReqdPropPlan *prppInput =  CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	m_pcrs =
		popPhysical->PcrsRequired(pmp, exprhdl, prppInput->PcrsRequired(), ulChildIndex, pdrgpdpCtxt, 0 /*ulOptReq*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::ComputeReqdCTEs
//
//	@doc:
//		Compute required CTEs
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::ComputeReqdCTEs
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdProp *prpInput,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt
	)
{
	GPOS_ASSERT(NULL == m_pcter);

	CReqdPropPlan *prppInput =  CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	m_pcter =
		popPhysical->PcteRequired(pmp, exprhdl, prppInput->Pcter(), ulChildIndex, pdrgpdpCtxt, 0 /*ulOptReq*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Compute
//
//	@doc:
//		Compute required props
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::Compute
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdProp *prpInput,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt,
	ULONG ulOptReq
	)
{
	GPOS_CHECK_ABORT;

	CReqdPropPlan *prppInput =  CReqdPropPlan::Prpp(prpInput);
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());
	ComputeReqdCols(pmp, exprhdl, prpInput, ulChildIndex, pdrgpdpCtxt);
	ComputeReqdCTEs(pmp, exprhdl, prpInput, ulChildIndex, pdrgpdpCtxt);
	CPartFilterMap *ppfmDerived = PpfmCombineDerived(pmp, exprhdl, prppInput, ulChildIndex, pdrgpdpCtxt);

	ULONG ulOrderReq = 0;
	ULONG ulDistrReq = 0;
	ULONG ulRewindReq = 0;
	ULONG ulPartPropagateReq = 0;
	popPhysical->LookupRequest(ulOptReq, &ulOrderReq, &ulDistrReq, &ulRewindReq, &ulPartPropagateReq);
	
	m_peo = GPOS_NEW(pmp) CEnfdOrder
						(
						popPhysical->PosRequired
							(
							pmp,
							exprhdl,
							prppInput->Peo()->PosRequired(),
							ulChildIndex,
							pdrgpdpCtxt,
							ulOrderReq
							),
						popPhysical->Eom(prppInput, ulChildIndex, pdrgpdpCtxt, ulOrderReq)
						);

	m_ped = GPOS_NEW(pmp) CEnfdDistribution
						(
						popPhysical->PdsRequired
							(
							pmp,
							exprhdl,
							prppInput->Ped()->PdsRequired(),
							ulChildIndex,
							pdrgpdpCtxt,
							ulDistrReq
							),
							popPhysical->Edm(prppInput, ulChildIndex, pdrgpdpCtxt, ulDistrReq)
						);

	GPOS_ASSERT(CDistributionSpec::EdtUniversal != m_ped->PdsRequired()->Edt() && "CDistributionSpecUniversal is a derive-only, cannot be required");

	m_per = GPOS_NEW(pmp) CEnfdRewindability
							(
							popPhysical->PrsRequired
								(
								pmp,
								exprhdl,
								prppInput->Per()->PrsRequired(),
								ulChildIndex,
								pdrgpdpCtxt,
								ulRewindReq
								),
							popPhysical->Erm(prppInput, ulChildIndex, pdrgpdpCtxt, ulRewindReq)
							);
	
	m_pepp = GPOS_NEW(pmp) CEnfdPartitionPropagation
							(
							popPhysical->PppsRequired
								(
								pmp, 
								exprhdl, 
								prppInput->Pepp()->PppsRequired(), 
								ulChildIndex, 
								pdrgpdpCtxt, 
								ulPartPropagateReq
								),
							CEnfdPartitionPropagation::EppmSatisfy,
							ppfmDerived
							);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PpfmCombineDerived
//
//	@doc:
//		Combine derived part filter map from input requirements and
//		derived plan properties in the passed context
//
//---------------------------------------------------------------------------
CPartFilterMap *
CReqdPropPlan::PpfmCombineDerived
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *prppInput,
	ULONG ulChildIndex,
	DrgPdp *pdrgpdpCtxt
	)
{
	// get partitioning info below required child
	CPartInfo *ppartinfo = exprhdl.Pdprel(ulChildIndex)->Ppartinfo();
	const ULONG ulConsumers = ppartinfo->UlConsumers();

	CPartFilterMap *ppfmDerived = GPOS_NEW(pmp) CPartFilterMap(pmp);

	// a bit set of found scan id's with part filters
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp);

	// copy part filters from input requirements
	for (ULONG ul = 0; ul < ulConsumers; ul++)
	{
		ULONG ulScanId = ppartinfo->UlScanId(ul);
		BOOL fCopied = ppfmDerived->FCopyPartFilter(pmp, ulScanId, prppInput->Pepp()->PpfmDerived());
		if (fCopied)
		{
#ifdef GPOS_DEBUG
			BOOL fSet =
#endif // GPOS_DEBUG
				pbs->FExchangeSet(ulScanId);
			GPOS_ASSERT(!fSet);
		}
	}

	// copy part filters from previously optimized children
	const ULONG ulSize = pdrgpdpCtxt->UlLength();
	for (ULONG ulDrvdProps = 0; ulDrvdProps < ulSize; ulDrvdProps++)
	{
		CDrvdPropPlan *pdpplan = CDrvdPropPlan::Pdpplan((*pdrgpdpCtxt)[ulDrvdProps]);
		for (ULONG ul = 0; ul < ulConsumers; ul++)
		{
			ULONG ulScanId = ppartinfo->UlScanId(ul);
			BOOL fFound = pbs->FBit(ulScanId);

			if (!fFound)
			{
				BOOL fCopied = ppfmDerived->FCopyPartFilter(pmp, ulScanId, pdpplan->Ppfm());
				if (fCopied)
				{
#ifdef GPOS_DEBUG
					BOOL fSet =
#endif // GPOS_DEBUG
						pbs->FExchangeSet(ulScanId);
					GPOS_ASSERT(!fSet);
				}
			}
		}
	}

	pbs->Release();

	return ppfmDerived;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::InitReqdPartitionPropagation
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
void
CReqdPropPlan::InitReqdPartitionPropagation
	(
	IMemoryPool *pmp, 
	CPartInfo *ppartinfo
	)
{
	GPOS_ASSERT(NULL == m_pepp && "Required Partition Propagation has been initialized already");
	
	CPartIndexMap *ppim = GPOS_NEW(pmp) CPartIndexMap(pmp);
	
	CEnfdPartitionPropagation::EPartitionPropagationMatching eppm = CEnfdPartitionPropagation::EppmSatisfy;
	for (ULONG ul = 0; ul < ppartinfo->UlConsumers(); ul++)
	{
		ULONG ulScanId = ppartinfo->UlScanId(ul);
		IMDId *pmdid = ppartinfo->PmdidRel(ul);
		DrgPpartkeys *pdrgppartkeys = ppartinfo->Pdrgppartkeys(ul);
		CPartConstraint *ppartcnstr = ppartinfo->Ppartcnstr(ul);

		pmdid->AddRef();
		pdrgppartkeys->AddRef();
		ppartcnstr->AddRef();

		ppim->Insert
			(
			ulScanId,
			GPOS_NEW(pmp) PartCnstrMap(pmp), 
			CPartIndexMap::EpimConsumer,
			0, //ulExpectedPropagators
			pmdid,
			pdrgppartkeys,
			ppartcnstr
			);
	}
	
	m_pepp =
		GPOS_NEW(pmp) CEnfdPartitionPropagation
			(
			GPOS_NEW(pmp) CPartitionPropagationSpec
				(
				ppim,
				GPOS_NEW(pmp) CPartFilterMap(pmp)
				),
			eppm,
			GPOS_NEW(pmp) CPartFilterMap(pmp)  // derived part filter map
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::Pps
//
//	@doc:
//		Given a property spec type, return the corresponding property spec
//		member
//
//---------------------------------------------------------------------------
CPropSpec *
CReqdPropPlan::Pps
	(
	ULONG ul
	)
	const
{
	CPropSpec::EPropSpecType epst = (CPropSpec::EPropSpecType) ul;
	switch(epst)
	{
		case CPropSpec::EpstOrder:
			return m_peo->PosRequired();

		case CPropSpec::EpstDistribution:
			 return m_ped->PdsRequired();

		case CPropSpec::EpstRewindability:
			return m_per->PrsRequired();

		case CPropSpec::EpstPartPropagation:
			if (NULL != m_pepp)
			{
				return m_pepp->PppsRequired();
			}
			return NULL;

		default:
			GPOS_ASSERT(!"Invalid property spec index");
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqual
//
//	@doc:
//		Check if expression attached to handle provides required columns
//		by all plan properties
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FProvidesReqdCols
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	ULONG ulOptReq
	)
	const
{
	CPhysical *popPhysical = CPhysical::PopConvert(exprhdl.Pop());

	// check if operator provides required columns
	if (!popPhysical->FProvidesReqdCols(exprhdl, m_pcrs, ulOptReq))
	{
		return false;
	}

	CColRefSet *pcrsOutput = exprhdl.Pdprel()->PcrsOutput();

	// check if property spec members use columns from operator output
	BOOL fProvidesReqdCols = true;
	for (ULONG ul = 0; fProvidesReqdCols && ul < CPropSpec::EpstSentinel; ul++)
	{
		CPropSpec *pps = Pps(ul);
		if (NULL == pps)
		{
			continue;
		}

		CColRefSet *pcrsUsed = pps->PcrsUsed(pmp);
		fProvidesReqdCols = pcrsOutput->FSubset(pcrsUsed);
		pcrsUsed->Release();
	}

	return fProvidesReqdCols;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FEqual
	(
	const CReqdPropPlan *prpp
	)
	const
{
	GPOS_ASSERT(NULL != prpp);

	BOOL fResult = 
		   PcrsRequired()->FEqual(prpp->PcrsRequired()) &&
	       Pcter()->FEqual(prpp->Pcter()) &&
	       Peo()->FMatch(prpp->Peo()) &&
	       Ped()->FMatch(prpp->Ped()) &&
	       Per()->FMatch(prpp->Per());

	if (fResult)
	{
		if (NULL == Pepp() || NULL == prpp->Pepp())
		{
			fResult = (NULL == Pepp() && NULL == prpp->Pepp());
		}
		else
		{
			fResult = Pepp()->FMatch(prpp->Pepp());
		}
	}

	return fResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::UlHash
//
//	@doc:
//		Compute hash value using required columns and required sort order
//
//---------------------------------------------------------------------------
ULONG
CReqdPropPlan::UlHash() const
{
	GPOS_ASSERT(NULL != m_pcrs);
	GPOS_ASSERT(NULL != m_peo);
	GPOS_ASSERT(NULL != m_ped);
	GPOS_ASSERT(NULL != m_per);
	GPOS_ASSERT(NULL != m_pcter);

	ULONG ulHash = m_pcrs->UlHash();
	ulHash = gpos::UlCombineHashes(ulHash, m_peo->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, m_ped->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, m_per->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, m_pcter->UlHash());

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FSatisfied
//
//	@doc:
//		Check if plan properties are satisfied by the given derived properties
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FSatisfied
	(
	const CDrvdPropRelational *pdprel,
	const CDrvdPropPlan *pdpplan
	)
	const
{
	GPOS_ASSERT(NULL != pdprel);
	GPOS_ASSERT(NULL != pdpplan);

	// first, check satisfiability of relational properties
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}
	
	// second, check satisfiability of plan properties;
	// if max cardinality <= 1, then any order requirement is already satisfied;
	// we only need to check satisfiability of distribution and rewindability
	if (pdprel->Maxcard().Ull() <= 1)
	{
		GPOS_ASSERT(NULL != pdpplan->Ppim());
		
		return
			pdpplan->Pds()->FSatisfies(this->Ped()->PdsRequired()) &&
			pdpplan->Prs()->FSatisfies(this->Per()->PrsRequired()) &&
			pdpplan->Ppim()->FSatisfies(this->Pepp()->PppsRequired()) &&
			pdpplan->Pcm()->FSatisfies(this->Pcter());
	}

	// otherwise, check satisfiability of all plan properties
	return pdpplan->FSatisfies(this);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FCompatible
//
//	@doc:
//		Check if plan properties are compatible with the given derived properties
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FCompatible
	(
	CExpressionHandle &exprhdl,
	CPhysical *popPhysical,
	const CDrvdPropRelational *pdprel,
	const CDrvdPropPlan *pdpplan
	)
	const
{
	GPOS_ASSERT(NULL != pdpplan);
	GPOS_ASSERT(NULL != pdprel);

	// first, check satisfiability of relational properties, including required columns
	if (!pdprel->FSatisfies(this))
	{
		return false;
	}

	return m_peo->FCompatible(pdpplan->Pos()) &&
	       m_ped->FCompatible(pdpplan->Pds()) &&
	       m_per->FCompatible(pdpplan->Prs()) &&
	       pdpplan->Ppim()->FSatisfies(m_pepp->PppsRequired()) &&
	       popPhysical->FProvidesReqdCTEs(exprhdl, m_pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppEmpty
//
//	@doc:
//		Generate empty required properties
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CReqdPropPlan::PrppEmpty
	(
	IMemoryPool *pmp
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	CDistributionSpec *pds = GPOS_NEW(pmp) CDistributionSpecAny(COperator::EopSentinel);
	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);
	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
	CCTEReq *pcter = GPOS_NEW(pmp) CCTEReq(pmp);

	return GPOS_NEW(pmp) CReqdPropPlan(pcrs, peo, ped, per, pcter);
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CReqdPropPlan::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "req cols: [";
	if (NULL != m_pcrs)
	{
		os << (*m_pcrs);
	}

	os << "], req CTEs: [";
	if (NULL != m_pcter)
	{
		os << (*m_pcter);
	}

	os << "], req order: [";
	if (NULL != m_peo)
	{
		os << (*m_peo);
	}

	os << "], req dist: [";
	if (NULL != m_ped)
	{
		os << (*m_ped);
	}

	os << "], req rewind: [";
	if (NULL != m_per)
	{
		os << "], req rewind: [" << (*m_per);
	}

	os << "], req partition propagation: [";
	if (NULL != m_pepp)
	{
		os << pp(m_pepp);
	}
	os <<  "]";
	
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::UlHashForCostBounding
//
//	@doc:
//		Hash function used for cost bounding
//
//---------------------------------------------------------------------------
ULONG
CReqdPropPlan::UlHashForCostBounding
	(
	const CReqdPropPlan *prpp
	)
{
	GPOS_ASSERT(NULL != prpp);

	ULONG ulHash = prpp->PcrsRequired()->UlHash();

	if (NULL != prpp->Ped())
	{
		ulHash = UlCombineHashes(ulHash, prpp->Ped()->UlHash());
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::FEqualForCostBounding
//
//	@doc:
//		Equality function used for cost bounding
//
//---------------------------------------------------------------------------
BOOL
CReqdPropPlan::FEqualForCostBounding
	(
	const CReqdPropPlan *prppFst,
	const CReqdPropPlan *prppSnd
	)
{
	GPOS_ASSERT(NULL != prppFst);
	GPOS_ASSERT(NULL != prppSnd);

	if (NULL == prppFst->Ped() || NULL == prppSnd->Ped())
	{
		return
			NULL == prppFst->Ped() &&
			NULL == prppSnd->Ped() &&
			prppFst->PcrsRequired()->FEqual(prppSnd->PcrsRequired());
	}

	return
		prppFst->PcrsRequired()->FEqual(prppSnd->PcrsRequired()) &&
		prppFst->Ped()->FMatch(prppSnd->Ped());
}


//---------------------------------------------------------------------------
//	@function:
//		CReqdPropPlan::PrppRemap
//
//	@doc:
//		Map input required and derived plan properties into new required
//		plan properties
//
//---------------------------------------------------------------------------
CReqdPropPlan *
CReqdPropPlan::PrppRemap
	(
	IMemoryPool *pmp,
	CReqdPropPlan *prppInput,
	CDrvdPropPlan *pdpplanInput,
	HMUlCr *phmulcr
	)
{
	GPOS_ASSERT(NULL != phmulcr);
	GPOS_ASSERT(NULL != prppInput);
	GPOS_ASSERT(NULL != pdpplanInput);

	// remap derived sort order to a required sort order

	COrderSpec *pos = pdpplanInput->Pos()->PosCopyWithRemappedColumns(pmp, phmulcr, false /*fMustExist*/);
	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder (pos, prppInput->Peo()->Eom());

	// remap derived distribution only if it can be used as required distribution

	CDistributionSpec *pdsDerived = pdpplanInput->Pds();
	CEnfdDistribution *ped = NULL;
	if (pdsDerived->FRequirable())
	{
		CDistributionSpec *pds = pdsDerived->PdsCopyWithRemappedColumns(pmp, phmulcr, false /*fMustExist*/);
		ped = GPOS_NEW(pmp) CEnfdDistribution(pds, prppInput->Ped()->Edm());
	}
	else
	{
		prppInput->Ped()->AddRef();
		ped = prppInput->Ped();
	}

	// other properties are copied from input

	prppInput->PcrsRequired()->AddRef();
	CColRefSet *pcrsRequired = prppInput->PcrsRequired();

	prppInput->Per()->AddRef();
	CEnfdRewindability *per = prppInput->Per();

	prppInput->Pepp()->AddRef();
	CEnfdPartitionPropagation *pepp = prppInput->Pepp();

	prppInput->Pcter()->AddRef();
	CCTEReq *pcter = prppInput->Pcter();

	return GPOS_NEW(pmp) CReqdPropPlan(pcrsRequired, peo, ped, per, pepp, pcter);
}

#ifdef GPOS_DEBUG
void
CReqdPropPlan::DbgPrint() const
{
	IMemoryPool *pmp = COptCtxt::PoctxtFromTLS()->Pmp();
	CAutoTrace at(pmp);
	(void) this->OsPrint(at.Os());
}
#endif // GPOS_DEBUG
// EOF
