//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPhysicalScan.cpp
//
//	@doc:
//		Implementation of base scan operator
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CCastUtils.h"
#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecRandom.h"

#include "gpopt/operators/CPhysicalScan.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/metadata/CName.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::CPhysicalScan
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CPhysicalScan::CPhysicalScan
	(
	IMemoryPool *pmp,
	const CName *pnameAlias,
	CTableDescriptor *ptabdesc,
	DrgPcr *pdrgpcrOutput
	)
	:
	CPhysical(pmp),
	m_pnameAlias(pnameAlias),
	m_ptabdesc(ptabdesc),
	m_pdrgpcrOutput(pdrgpcrOutput),
	m_pds(NULL),
	m_pstatsBaseTable(NULL)
{
	GPOS_ASSERT(NULL != ptabdesc);
	GPOS_ASSERT(NULL != pnameAlias);
	GPOS_ASSERT(NULL != pdrgpcrOutput);

	if (ptabdesc->FConvertHashToRandom())
	{
		// Treating a hash distributed table as random during planning
		m_pds = GPOS_NEW(m_pmp) CDistributionSpecRandom();
	}
	else
	{
		m_pds = CPhysical::PdsCompute(m_pmp, ptabdesc, pdrgpcrOutput);
	}
	ComputeTableStats(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::~CPhysicalScan
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CPhysicalScan::~CPhysicalScan()
{
	m_ptabdesc->Release();
	m_pdrgpcrOutput->Release();
	m_pds->Release();
	m_pstatsBaseTable->Release();
	GPOS_DELETE(m_pnameAlias);
}

	
//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::FInputOrderSensitive
//
//	@doc:
//		Not called for leaf operators
//
//---------------------------------------------------------------------------
BOOL
CPhysicalScan::FInputOrderSensitive() const
{
	GPOS_ASSERT(!"Unexpected function call of FInputOrderSensitive");
	return false;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::FProvidesReqdCols
//
//	@doc:
//		Check if required columns are included in output columns
//
//---------------------------------------------------------------------------
BOOL
CPhysicalScan::FProvidesReqdCols
	(
	CExpressionHandle &, // exprhdl
	CColRefSet *pcrsRequired,
	ULONG // ulOptReq
	)
	const
{
	GPOS_ASSERT(NULL != pcrsRequired);

	CColRefSet 	*pcrs = GPOS_NEW(m_pmp) CColRefSet(m_pmp);
	pcrs->Include(m_pdrgpcrOutput);

	BOOL fResult = pcrs->FSubset(pcrsRequired);
	pcrs->Release();
	
	return fResult;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::EpetOrder
//
//	@doc:
//		Return the enforcing type for order property based on this operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalScan::EpetOrder
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

	return CEnfdProp::EpetRequired;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PexprMatchEqualitySide
//
//	@doc:
//		Search the given array of predicates for an equality predicate
//		that has one side equal to the given expression,
//		if found, return the other side of equality, otherwise return NULL
//
//---------------------------------------------------------------------------
CExpression *
CPhysicalScan::PexprMatchEqualitySide
	(
	CExpression *pexprToMatch,
	DrgPexpr *pdrgpexpr // array of predicates to inspect
	)
{
	GPOS_ASSERT(NULL != pexprToMatch);
	GPOS_ASSERT(NULL != pdrgpexpr);

	CExpression *pexprMatching = NULL;
	const ULONG ulSize = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprPred = (*pdrgpexpr)[ul];
		if (!CPredicateUtils::FEquality(pexprPred))
		{
			continue;
		}

		// extract equality sides
		CExpression *pexprPredOuter = (*pexprPred)[0];
		CExpression *pexprPredInner = (*pexprPred)[1];

		IMDId *pmdidTypeOuter = CScalar::PopConvert(pexprPredOuter->Pop())->PmdidType();
		IMDId *pmdidTypeInner = CScalar::PopConvert(pexprPredInner->Pop())->PmdidType();
		if (!pmdidTypeOuter->FEquals(pmdidTypeInner))
		{
			// only consider equality of identical types
			continue;
		}

		pexprToMatch = CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprToMatch);
		if (CUtils::FEqual(CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredOuter), pexprToMatch))
		{
			pexprMatching = pexprPredInner;
			break;
		}

		if (CUtils::FEqual(CCastUtils::PexprWithoutBinaryCoercibleCasts(pexprPredInner), pexprToMatch))
		{
			pexprMatching = pexprPredOuter;
			break;
		}
	}

	return pexprMatching;
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PdshashedDeriveWithOuterRefs
//
//	@doc:
//		Derive hashed distribution when predicates have outer references
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CPhysicalScan::PdshashedDeriveWithOuterRefs
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	GPOS_ASSERT(exprhdl.FHasOuterRefs());
	GPOS_ASSERT(CDistributionSpec::EdtHashed == m_pds->Edt());

	CExpression *pexprIndexPred = exprhdl.PexprScalarChild(0 /*ulChildIndex*/);
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprIndexPred);

	DrgPexpr *pdrgpexprMatching = GPOS_NEW(pmp) DrgPexpr(pmp);
	CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(m_pds);
	DrgPexpr *pdrgpexprHashed = pdshashed->Pdrgpexpr();
	const ULONG ulSize = pdrgpexprHashed->UlLength();

	BOOL fSuccess = true;
	for (ULONG ul = 0; fSuccess && ul < ulSize; ul++)
	{
		CExpression *pexpr = (*pdrgpexprHashed)[ul];
		CExpression *pexprMatching = PexprMatchEqualitySide(pexpr, pdrgpexpr);
		fSuccess = (NULL != pexprMatching);
		if (fSuccess)
		{
			pexprMatching->AddRef();
			pdrgpexprMatching->Append(pexprMatching);
		}
	}
	pdrgpexpr->Release();

	if (fSuccess)
	{
		GPOS_ASSERT(pdrgpexprMatching->UlLength() == pdrgpexprHashed->UlLength());

		// create a matching hashed distribution request
		BOOL fNullsColocated = pdshashed->FNullsColocated();
		CDistributionSpecHashed *pdshashedEquiv = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprMatching, fNullsColocated);

		pdrgpexprHashed->AddRef();
		CDistributionSpecHashed *pdshashedResult = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprHashed, fNullsColocated, pdshashedEquiv);

		return pdshashedResult;
	}

	pdrgpexprMatching->Release();

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PdsDerive
//
//	@doc:
//		Derive distribution
//
//---------------------------------------------------------------------------
CDistributionSpec *
CPhysicalScan::PdsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	BOOL fIndexOrBitmapScan = COperator::EopPhysicalIndexScan == Eopid() ||
				COperator::EopPhysicalBitmapTableScan == Eopid();
	if (fIndexOrBitmapScan &&
		CDistributionSpec::EdtHashed == m_pds->Edt() &&
		exprhdl.FHasOuterRefs())
	{
		// if index conditions have outer references and index relation is hashed,
		// check to see if we can derive an equivalent hashed distribution for the
		// outer references
		CDistributionSpecHashed *pdshashed = PdshashedDeriveWithOuterRefs(pmp, exprhdl);
		if (NULL != pdshashed)
		{
			return pdshashed;
		}
	}

	m_pds->AddRef();

	return m_pds;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PpimDeriveFromDynamicScan
//
//	@doc:
//		Derive partition index map from a dynamic scan operator
//
//---------------------------------------------------------------------------
CPartIndexMap *
CPhysicalScan::PpimDeriveFromDynamicScan
	(
	IMemoryPool *pmp,
	ULONG ulPartIndexId,
	IMDId *pmdidRel,
	DrgDrgPcr *pdrgpdrgpcrPart,
	ULONG ulSecondaryPartIndexId,
	CPartConstraint *ppartcnstr,
	CPartConstraint *ppartcnstrRel,
	ULONG ulExpectedPropagators
	)
{
	CPartIndexMap *ppim = GPOS_NEW(pmp) CPartIndexMap(pmp);
	PartCnstrMap *ppartcnstrmap = GPOS_NEW(pmp) PartCnstrMap(pmp);
	
	(void) ppartcnstrmap->FInsert(GPOS_NEW(pmp) ULONG(ulSecondaryPartIndexId), ppartcnstr);

	DrgPpartkeys *pdrgppartkeys = GPOS_NEW(pmp) DrgPpartkeys(pmp);
	pdrgppartkeys->Append(GPOS_NEW(pmp) CPartKeys(pdrgpdrgpcrPart));

	ppim->Insert(ulPartIndexId, ppartcnstrmap, CPartIndexMap::EpimConsumer, ulExpectedPropagators, pmdidRel, pdrgppartkeys, ppartcnstrRel);

	return ppim;
}

//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::EpetDistribution
//
//	@doc:
//		Return the enforcing type for distribution property based on this
//		operator
//
//---------------------------------------------------------------------------
CEnfdProp::EPropEnforcingType
CPhysicalScan::EpetDistribution
	(
	CExpressionHandle &/*exprhdl*/,
	const CEnfdDistribution *ped
	)
	const
{
	GPOS_ASSERT(NULL != ped);

	if (ped->FCompatible(m_pds))
	{
		// required distribution will be established by the operator
		return CEnfdProp::EpetUnnecessary;
	}

	// required distribution will be enforced on output
	return CEnfdProp::EpetRequired;
}



//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::ComputeTableStats
//
//	@doc:
//		Compute stats of underlying table
//
//---------------------------------------------------------------------------
void
CPhysicalScan::ComputeTableStats
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pstatsBaseTable);

	CColRefSet *pcrsHist = GPOS_NEW(pmp) CColRefSet(pmp);
	CColRefSet *pcrsWidth = GPOS_NEW(pmp) CColRefSet(pmp, m_pdrgpcrOutput);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	m_pstatsBaseTable = pmda->Pstats(pmp, m_ptabdesc->Pmdid(), pcrsHist, pcrsWidth);
	GPOS_ASSERT(NULL != m_pstatsBaseTable);

	pcrsHist->Release();
	pcrsWidth->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPhysicalScan::PopConvert
//
//	@doc:
//		Conversion function
//
//---------------------------------------------------------------------------
CPhysicalScan *
CPhysicalScan::PopConvert
	(
	COperator *pop
	)
{
	GPOS_ASSERT(NULL != pop);
	GPOS_ASSERT(CUtils::FPhysicalScan(pop));

	return dynamic_cast<CPhysicalScan*>(pop);
}


// EOF

