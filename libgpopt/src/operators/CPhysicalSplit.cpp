//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalSplit.cpp
//
//	@doc:
//		Implementation of physical split operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRandom.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPhysicalSplit.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::CPhysicalSplit
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalSplit::CPhysicalSplit
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcrDelete,
	DrgPcr *pdrgpcrInsert,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId,
	CColRef *pcrAction,
	CColRef *pcrTupleOid
	)
	:
	CPhysical(pmp),
	m_pdrgpcrDelete(pdrgpcrDelete),
	m_pdrgpcrInsert(pdrgpcrInsert),
	m_pcrCtid(pcrCtid),
	m_pcrSegmentId(pcrSegmentId),
	m_pcrAction(pcrAction),
	m_pcrTupleOid(pcrTupleOid),
	m_pcrsRequiredLocal(NULL)
{
	GPOS_ASSERT(NULL != pdrgpcrDelete);
	GPOS_ASSERT(NULL != pdrgpcrInsert);
	GPOS_ASSERT(pdrgpcrInsert->UlLength() == pdrgpcrDelete->UlLength());
	GPOS_ASSERT(NULL != pcrCtid);
	GPOS_ASSERT(NULL != pcrSegmentId);
	GPOS_ASSERT(NULL != pcrAction);

	m_pcrsRequiredLocal = GPOS_NEW(pmp) CColRefSet(pmp);
	m_pcrsRequiredLocal->Include(m_pdrgpcrDelete);
	m_pcrsRequiredLocal->Include(m_pdrgpcrInsert);
	if (NULL != m_pcrTupleOid)
	{
		m_pcrsRequiredLocal->Include(m_pcrTupleOid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::~CPhysicalSplit
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalSplit::~CPhysicalSplit()
{
	m_pdrgpcrDelete->Release();
	m_pdrgpcrInsert->Release();
	m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PosRequired
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSplit::PosRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	COrderSpec *, // posRequired
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

	// return empty sort order
	return GPOS_NEW(pmp) COrderSpec(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalSplit::PosDerive
	(
	IMemoryPool *, //pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	return PosDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSplit::EpetOrder
	(
	CExpressionHandle &, // exprhdl
	const CEnfdOrder *
#ifdef GPOS_DEBUG
	peo
#endif // GPOS_DEBUG
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());

	// always force sort to be on top of split
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalSplit::PcrsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl,
	CColRefSet *pcrsRequired,
	ULONG
#ifdef GPOS_DEBUG
	ulChildIndex
#endif // GPOS_DEBUG
	,
	DrgPdp *, // pdrgpdpCtxt
	ULONG // ulOptReq
	)
{
	GPOS_ASSERT(0 == ulChildIndex);

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, *m_pcrsRequiredLocal);
	pcrs->Union(pcrsRequired);
	pcrs->Exclude(m_pcrAction);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSplit::PdsRequired
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl,
	CDistributionSpec *, // pdsInput,
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

 	return GPOS_NEW(pmp) CDistributionSpecAny(this->Eopid());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSplit::PrsRequired
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

	return PrsPassThru(pmp, exprhdl, prsRequired, ulChildIndex);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalSplit::PppsRequired
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
//		CPhysicalSplit::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalSplit::PcteRequired
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
//		CPhysicalSplit::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSplit::FProvidesReqdCols
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
	// include defined column
	pcrs->Include(m_pcrAction);

	// include output columns of the relational child
	pcrs->Union(exprhdl.Pdprel(0 /*ulChildIndex*/)->PcrsOutput());

	BOOL fProvidesCols = pcrs->FSubset(pcrsRequired);
	pcrs->Release();

	return fProvidesCols;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalSplit::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	CDistributionSpec *pdsOuter = exprhdl.Pdpplan(0 /*ulChildIndex*/)->Pds();
	
	if (CDistributionSpec::EdtHashed != pdsOuter->Edt())
	{
		pdsOuter->AddRef();
		return pdsOuter;
	}
	
	// find out which columns of the target table get modified by the DML and check
	// whether those participate in the derived hash distribution
	CColRefSet *pcrsModified = GPOS_NEW(pmp) CColRefSet(pmp);
	const ULONG ulCols = m_pdrgpcrDelete->UlLength();
	
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcrOld = (*m_pdrgpcrDelete)[ul];
		CColRef *pcrNew = (*m_pdrgpcrInsert)[ul];
		if (pcrOld != pcrNew)
		{
			pcrsModified->Include(pcrOld);
			pcrsModified->Include(pcrNew);
		}
	}
	
	CDistributionSpecHashed *pdsHashed = CDistributionSpecHashed::PdsConvert(pdsOuter);
	CColRefSet *pcrsHashed = CUtils::PcrsExtractColumns(pmp, pdsHashed->Pdrgpexpr());
	
	if (!pcrsModified->FDisjoint(pcrsHashed))
	{
		pcrsModified->Release();
		pcrsHashed->Release();
		return GPOS_NEW(pmp) CDistributionSpecRandom();
	}
		
	if (NULL != pdsHashed->PdshashedEquiv())
	{
		CColRefSet *pcrsHashedEquiv = CUtils::PcrsExtractColumns(pmp, pdsHashed->PdshashedEquiv()->Pdrgpexpr());
		if (!pcrsModified->FDisjoint(pcrsHashedEquiv))
		{
			pcrsHashed->Release();
			pcrsHashedEquiv->Release();
			pcrsModified->Release();
			return GPOS_NEW(pmp) CDistributionSpecRandom();
		}
		pcrsHashedEquiv->Release();
	}
	
	pcrsModified->Release();
	pcrsHashed->Release();
	pdsHashed->AddRef();
	return pdsHashed;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalSplit::PrsDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PrsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalSplit::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), CUtils::UlHashColArray(m_pdrgpcrInsert));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrCtid));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrSegmentId));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrAction));

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::FMatch
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalSplit::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CPhysicalSplit *popSplit = CPhysicalSplit::PopConvert(pop);

		return m_pcrCtid == popSplit->PcrCtid() &&
				m_pcrSegmentId == popSplit->PcrSegmentId() &&
				m_pcrAction == popSplit->PcrAction() &&
				m_pcrTupleOid == popSplit->PcrTupleOid() &&
				m_pdrgpcrDelete->FEqual(popSplit->PdrgpcrDelete()) &&
				m_pdrgpcrInsert->FEqual(popSplit->PdrgpcrInsert());
	}

	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalSplit::EpetRewindability
	(
	CExpressionHandle &exprhdl,
	const CEnfdRewindability *per
	)
	const
{
	// get rewindability delivered by the split node
	CRewindabilitySpec *prs = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Prs();
	if (per->FCompatible(prs))
	{
		 // required rewindability is already provided
		 return CEnfdProp::EpetUnnecessary;
	}

	// always force spool to be on top of split
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalSplit::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalSplit::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os	<< SzId() << " -- Delete Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrDelete);
	os	<< "], Insert Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrInsert);
	os	<< "], ";
	m_pcrCtid->OsPrint(os);
	os	<< ", ";
	m_pcrSegmentId->OsPrint(os);
	os	<< ", Action: ";
	m_pcrAction->OsPrint(os);

	return os;
}


// EOF
