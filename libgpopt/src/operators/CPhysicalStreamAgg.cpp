//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CPhysicalStreamAgg.cpp
//
//	@doc:
//		Implementation of stream aggregation operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecSingleton.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalStreamAgg.h"


using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAgg::CPhysicalStreamAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalStreamAgg::CPhysicalStreamAgg
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	DrgPcr *pdrgpcrMinimal,
	COperator::EGbAggType egbaggtype,
	BOOL fGeneratesDuplicates,
	DrgPcr *pdrgpcrArgDQA,
	BOOL fMultiStage
	)
	:
	CPhysicalAgg(pmp, pdrgpcr, pdrgpcrMinimal, egbaggtype, fGeneratesDuplicates, pdrgpcrArgDQA, fMultiStage),
	m_pos(NULL)
{
	GPOS_ASSERT(NULL != m_pdrgpcrMinimal);
	m_pcrsMinimalGrpCols = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcrMinimal);
	InitOrderSpec(pmp, m_pdrgpcrMinimal);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAgg::InitOrderSpec
//
//	@doc:
//		Initialize the order spec using the given array of columns
//
//---------------------------------------------------------------------------
void
CPhysicalStreamAgg::InitOrderSpec
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrOrder
	)
{
	GPOS_ASSERT(NULL != pdrgpcrOrder);

	CRefCount::SafeRelease(m_pos);
	m_pos = GPOS_NEW(pmp) COrderSpec(pmp);
	const ULONG ulSize = pdrgpcrOrder->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CColRef *pcr = (*pdrgpcrOrder)[ul];

		// TODO: 12/21/2011 - ; this seems broken: a pcr must not embed
		// a pointer to a cached object
		gpmd::IMDId *pmdid = pcr->Pmdtype()->PmdidCmp(IMDType::EcmptL);
		pmdid->AddRef();

		m_pos->Append(pmdid, pcr, COrderSpec::EntLast);
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAgg::~CPhysicalStreamAgg
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalStreamAgg::~CPhysicalStreamAgg()
{
	m_pcrsMinimalGrpCols->Release();
	m_pos->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAgg::PosCovering
//
//	@doc:
//		Construct order spec on grouping column so that it covers required
//		order spec, the function returns NULL if no covering order spec
//		can be created
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalStreamAgg::PosCovering
	(
	IMemoryPool *pmp,
	COrderSpec *posRequired,
	DrgPcr *pdrgpcrGrp
	)
	const
{
	GPOS_ASSERT(NULL != posRequired);

	if (0 == posRequired->UlSortColumns())
	{
		// required order must be non-empty
		return NULL;
	}

	// create a set of required sort columns
	CColRefSet *pcrsReqd = posRequired->PcrsUsed(pmp);

	COrderSpec *pos = NULL;

	CColRefSet *pcrsGrpCols = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrGrp);
	if (pcrsGrpCols->FSubset(pcrsReqd))
	{
		// required order columns are included in grouping columns, we can
		// construct a covering order spec
		pos = GPOS_NEW(pmp) COrderSpec(pmp);

		// extract order expressions from required order
		const ULONG ulReqdSortCols = posRequired->UlSortColumns();
		for (ULONG ul = 0; ul < ulReqdSortCols; ul++)
		{
			CColRef *pcr = const_cast<CColRef *>(posRequired->Pcr(ul));
			IMDId *pmdid = posRequired->PmdidSortOp(ul);
			COrderSpec::ENullTreatment ent = posRequired->Ent(ul);
			pmdid->AddRef();
			pos->Append(pmdid, pcr, ent);
		}

		// augment order with remaining grouping columns
		const ULONG ulSize = pdrgpcrGrp->UlLength();
		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			CColRef *pcr = (*pdrgpcrGrp)[ul];
			if (!pcrsReqd->FMember(pcr))
			{
				IMDId *pmdid = pcr->Pmdtype()->PmdidCmp(IMDType::EcmptL);
				pmdid->AddRef();
				pos->Append(pmdid, pcr, COrderSpec::EntLast);
			}
		}
	}
	pcrsGrpCols->Release();
	pcrsReqd->Release();

	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAgg::PosRequiredStreamAgg
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalStreamAgg::PosRequiredStreamAgg
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	COrderSpec *posRequired,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPcr *pdrgpcrGrp
	)
	const
{
	GPOS_ASSERT(0 == ulChildIndex);

	COrderSpec *pos = PosCovering(pmp, posRequired, pdrgpcrGrp);
	if (NULL == pos)
	{
		// failed to find a covering order spec, use local order spec
		m_pos->AddRef();
		pos = m_pos;
	}

	// extract sort columns from order spec
	CColRefSet *pcrs = pos->PcrsUsed(pmp);

	// get key collection of the relational child
	CKeyCollection *pkc = exprhdl.Pdprel(0)->Pkc();

	if (NULL != pkc && pkc->FKey(pcrs, false /*fExactMatch*/))
	{
		CColRefSet *pcrsReqd = posRequired->PcrsUsed(m_pmp);
		BOOL fUsesDefinedCols = FUnaryUsesDefinedColumns(pcrsReqd, exprhdl);
		pcrsReqd->Release();
		
		if (!fUsesDefinedCols)
		{
			// we are grouping on child's key,
			// stream agg does not need to sort child and we can pass through input spec
			pos->Release();
			posRequired->AddRef();
			pos = posRequired;
		}
	}
	pcrs->Release();

	return pos;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalStreamAgg::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalStreamAgg::PosDerive
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
//		CPhysicalStreamAgg::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalStreamAgg::EpetOrder
	(
	CExpressionHandle &exprhdl,
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());
	
	// get the order delivered by the stream agg node
	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		// required order will be established by the stream agg operator
		return CEnfdProp::EpetUnnecessary;
	}
	
	// required order will be enforced on limit's output
	return CEnfdProp::EpetRequired;
}

// EOF

