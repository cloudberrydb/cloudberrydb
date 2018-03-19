//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalDML.cpp
//
//	@doc:
//		Implementation of physical DML operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CDistributionSpecAny.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/base/CDistributionSpecRouted.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CPhysicalDML.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::CPhysicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPhysicalDML::CPhysicalDML
	(
	IMemoryPool *pmp,
	CLogicalDML::EDMLOperator edmlop,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrSource,
	CBitSet *pbsModified,
	CColRef *pcrAction,
	CColRef *pcrTableOid,
	CColRef *pcrCtid,
	CColRef *pcrSegmentId,
	CColRef *pcrTupleOid
	)
	:
	CPhysical(pmp),
	m_edmlop(edmlop),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrSource(pdrgpcrSource),
	m_pbsModified(pbsModified),
	m_pcrAction(pcrAction),
	m_pcrTableOid(pcrTableOid),
	m_pcrCtid(pcrCtid),
	m_pcrSegmentId(pcrSegmentId),
	m_pcrTupleOid(pcrTupleOid),
	m_pds(NULL),
	m_pos(NULL),
	m_pcrsRequiredLocal(NULL),
	m_fInputSorted(false)
{
	GPOS_ASSERT(CLogicalDML::EdmlSentinel != edmlop);
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pdrgpcrSource);
	GPOS_ASSERT(NULL != pbsModified);
	GPOS_ASSERT(NULL != pcrAction);
	GPOS_ASSERT_IMP(CLogicalDML::EdmlDelete == edmlop || CLogicalDML::EdmlUpdate == edmlop,
					NULL != pcrCtid && NULL != pcrSegmentId);

	m_pds = CPhysical::PdsCompute(m_pmp, m_ptabdesc, pdrgpcrSource);
	m_pos = PosComputeRequired(pmp, ptabdesc);
	ComputeRequiredLocalColumns(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::~CPhysicalDML
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPhysicalDML::~CPhysicalDML()
{
	m_ptabdesc->Release();
	m_pdrgpcrSource->Release();
	m_pbsModified->Release();
	m_pds->Release();
	m_pos->Release();
	m_pcrsRequiredLocal->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PosRequired
//
//	@doc:
//		Compute required sort columns of the n-th child
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalDML::PosRequired
	(
	IMemoryPool *, // pmp
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
	m_pos->AddRef();
	return m_pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PosDerive
//
//	@doc:
//		Derive sort order
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalDML::PosDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle & // exprhdl
	)
	const
{
	// return empty sort order
	return GPOS_NEW(pmp) COrderSpec(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalDML::EpetOrder
	(
	CExpressionHandle &exprhdl,
	const CEnfdOrder *peo
	)
	const
{
	GPOS_ASSERT(NULL != peo);
	GPOS_ASSERT(!peo->PosRequired()->FEmpty());
	
	// get the order delivered by the DML node
	COrderSpec *pos = CDrvdPropPlan::Pdpplan(exprhdl.Pdp())->Pos();
	if (peo->FCompatible(pos))
	{
		return CEnfdProp::EpetUnnecessary;
	}
	
	// required order will be enforced on limit's output
	return CEnfdProp::EpetRequired;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PcrsRequired
//
//	@doc:
//		Compute required columns of the n-th child;
//		we only compute required columns for the relational child;
//
//---------------------------------------------------------------------------
CColRefSet *
CPhysicalDML::PcrsRequired
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
	GPOS_ASSERT(0 == ulChildIndex &&
				"Required properties can only be computed on the relational child");

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, *m_pcrsRequiredLocal);
	pcrs->Union(pcrsRequired);

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PdsRequired
//
//	@doc:
//		Compute required distribution of the n-th child
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalDML::PdsRequired
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
	
	if (CDistributionSpec::EdtRandom == m_pds->Edt() && CLogicalDML::EdmlInsert != m_edmlop)
	{
		return GPOS_NEW(pmp) CDistributionSpecRouted(m_pcrSegmentId);
	}
	
	m_pds->AddRef();
	return m_pds;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PrsRequired
//
//	@doc:
//		Compute required rewindability of the n-th child
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalDML::PrsRequired
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
//		CPhysicalDML::PppsRequired
//
//	@doc:
//		Compute required partition propagation of the n-th child
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec *
CPhysicalDML::PppsRequired
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
//		CPhysicalDML::PcteRequired
//
//	@doc:
//		Compute required CTE map of the n-th child
//
//---------------------------------------------------------------------------
CCTEReq *
CPhysicalDML::PcteRequired
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
//		CPhysicalDML::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDML::FProvidesReqdCols
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
//		CPhysicalDML::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalDML::PdsDerive
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PdsDerivePassThruOuter(exprhdl);
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PrsDerive
//
//	@doc:
//		Derive rewindability
//
//---------------------------------------------------------------------------
CRewindabilitySpec *
CPhysicalDML::PrsDerive
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
//		CPhysicalDML::UlHash
//
//	@doc:
//		Operator specific hash function
//
//---------------------------------------------------------------------------
ULONG
CPhysicalDML::UlHash() const
{
	ULONG ulHash = gpos::UlCombineHashes(COperator::UlHash(), m_ptabdesc->Pmdid()->UlHash());
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrAction));
	ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrTableOid));
	ulHash = gpos::UlCombineHashes(ulHash, CUtils::UlHashColArray(m_pdrgpcrSource));
	
	if (CLogicalDML::EdmlDelete == m_edmlop || CLogicalDML::EdmlUpdate == m_edmlop)
	{
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrCtid));
		ulHash = gpos::UlCombineHashes(ulHash, gpos::UlHashPtr<CColRef>(m_pcrSegmentId));
	}

	return ulHash;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::FMatch
//
//	@doc:
//		Match operator
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDML::FMatch
	(
	COperator *pop
	)
	const
{
	if (pop->Eopid() == Eopid())
	{
		CPhysicalDML *popDML = CPhysicalDML::PopConvert(pop);
		
		return m_pcrAction == popDML->PcrAction() &&
				m_pcrTableOid == popDML->PcrTableOid() &&
				m_pcrCtid == popDML->PcrCtid() &&
				m_pcrSegmentId == popDML->PcrSegmentId() &&
				m_pcrTupleOid == popDML->PcrTupleOid() &&
				m_ptabdesc->Pmdid()->FEquals(popDML->Ptabdesc()->Pmdid()) &&
				m_pdrgpcrSource->FEqual(popDML->PdrgpcrSource());
	}
	
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::EpetRewindability
//
//	@doc:
//		Return the enforcing type for rewindability property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalDML::EpetRewindability
	(
	CExpressionHandle &, // exprhdl,
	const CEnfdRewindability * // per
	)
	const
{
	return CEnfdProp::EpetProhibited;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::PosComputeRequired
//
//	@doc:
//		Compute required sort order based on the key information in the table 
//		descriptor:
//		1. If a table has no keys, no sort order is necessary.
//
//		2. If a table has keys, but they are not modified in the update, no sort
//		order is necessary. This relies on the fact that Split always produces
//		Delete tuples before Insert tuples, so we cannot have two versions of the
//		same tuple on the same time. Consider for example tuple (A: 1, B: 2), where
//		A is key and an update "set B=B+1". Since there cannot be any other tuple 
//		with A=1, and the tuple (1,2) is deleted before tuple (1,3) gets inserted,
//		we don't need to enforce specific order of deletes and inserts.
//
//		3. If the update changes a key column, enforce order on the Action column
//		to deliver Delete tuples before Insert tuples. This is done to avoid a
//		conflict between a newly inserted tuple and an old tuple that is about to be
//		deleted. Consider table with tuples (A: 1),(A: 2), where A is key, and 
//		update "set A=A+1". Split will generate tuples (1,"D"), (2,"I"), (2,"D"), (3,"I").
//		If (2,"I") happens before (2,"D") we will have a violation of the key constraint.
//		Therefore we need to enforce sort order on Action to get all old tuples
//		tuples deleted before the new ones are inserted.
//
//---------------------------------------------------------------------------
COrderSpec *
CPhysicalDML::PosComputeRequired
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc
	)
{
	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);

	const DrgPbs *pdrgpbsKeys = ptabdesc->PdrgpbsKeys();
	if (1 < pdrgpbsKeys->UlLength() && CLogicalDML::EdmlUpdate == m_edmlop)
	{
		// if this is an update on the target table's keys, enforce order on 
		// the action column, see explanation in function's comment		
		const ULONG ulKeySets = pdrgpbsKeys->UlLength();
		BOOL fNeedsSort = false;
		for (ULONG ul = 0; ul < ulKeySets && !fNeedsSort; ul++)
		{
			CBitSet *pbs = (*pdrgpbsKeys)[ul];
			if (!pbs->FDisjoint(m_pbsModified))
			{
				fNeedsSort = true;
				break;
			}
		}
		
		if (fNeedsSort)
		{
			IMDId *pmdid = m_pcrAction->Pmdtype()->PmdidCmp(IMDType::EcmptL);
			pmdid->AddRef();
			pos->Append(pmdid, m_pcrAction, COrderSpec::EntAuto);
		}
	}
	else if (m_ptabdesc->FPartitioned())
	{
		COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();

		BOOL fInsertSortOnParquet = FInsertSortOnParquet();
		BOOL fInsertSortOnRows = FInsertSortOnRows(poconf);

		if (fInsertSortOnParquet || fInsertSortOnRows)
		{
			GPOS_ASSERT(CLogicalDML::EdmlInsert == m_edmlop);
			m_fInputSorted = true;
			// if this is an INSERT over a partitioned Parquet or Row-oriented table,
			// sort tuples by their table oid
			IMDId *pmdid = m_pcrTableOid->Pmdtype()->PmdidCmp(IMDType::EcmptL);
			pmdid->AddRef();
			pos->Append(pmdid, m_pcrTableOid, COrderSpec::EntAuto);
		}
	}
	
	return pos;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::FInsertSortOnParquet
//
//	@doc:
//		Do we need to sort on parquet table
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDML::FInsertSortOnParquet()
{
	return !GPOS_FTRACE(EopttraceDisableSortForDMLOnParquet) &&
					(IMDRelation::ErelstorageAppendOnlyParquet == m_ptabdesc->Erelstorage());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::FInsertSortOnRows
//
//	@doc:
//		Do we need to sort on insert
//
//---------------------------------------------------------------------------
BOOL
CPhysicalDML::FInsertSortOnRows
	(
	COptimizerConfig *poconf
	)
{
	GPOS_ASSERT(NULL != poconf);

	return (IMDRelation::ErelstorageAppendOnlyRows == m_ptabdesc->Erelstorage()) &&
			(poconf->Phint()->UlMinNumOfPartsToRequireSortOnInsert() <= m_ptabdesc->UlPartitions());
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::ComputeRequiredLocalColumns
//
//	@doc:
//		Compute a set of columns required by local members
//
//---------------------------------------------------------------------------
void
CPhysicalDML::ComputeRequiredLocalColumns
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pcrsRequiredLocal);

	m_pcrsRequiredLocal = GPOS_NEW(pmp) CColRefSet(pmp);

	// include source columns
	m_pcrsRequiredLocal->Include(m_pdrgpcrSource);
	m_pcrsRequiredLocal->Include(m_pcrAction);

	if (m_pcrTableOid != NULL)
	{
		m_pcrsRequiredLocal->Include(m_pcrTableOid);
	}

	if (CLogicalDML::EdmlDelete == m_edmlop || CLogicalDML::EdmlUpdate == m_edmlop)
	{
		m_pcrsRequiredLocal->Include(m_pcrCtid);
		m_pcrsRequiredLocal->Include(m_pcrSegmentId);
	}

	if (NULL != m_pcrTupleOid)
	{
		m_pcrsRequiredLocal->Include(m_pcrTupleOid);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalDML::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPhysicalDML::OsPrint
	(
	IOstream &os
	)
	const
{
	if (m_fPattern)
	{
		return COperator::OsPrint(os);
	}

	os	<< SzId()
		<< " (";
	os << CLogicalDML::m_rgwszDml[m_edmlop] << ", ";
	m_ptabdesc->Name().OsPrint(os);
	os << "), Source Columns: [";
	CUtils::OsPrintDrgPcr(os, m_pdrgpcrSource);
	os	<< "], Action: (";
	m_pcrAction->OsPrint(os);
	os << ")";

	if (m_pcrTableOid != NULL)
	{
		os << ", Oid: (";
		m_pcrTableOid->OsPrint(os);
		os << ")";
	}

	if (CLogicalDML::EdmlDelete == m_edmlop || CLogicalDML::EdmlUpdate == m_edmlop)
	{
		os << ", ";
		m_pcrCtid->OsPrint(os);
		os	<< ", ";
		m_pcrSegmentId->OsPrint(os);
	}


	return os;
}

// EOF
