//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPartitionPropagationSpec.cpp
//
//	@doc:
//		Specification of partition propagation requirements
//---------------------------------------------------------------------------

#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/exception.h"

#include "gpopt/base/CPartitionPropagationSpec.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/operators/CPhysicalPartitionSelector.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpos;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::CPartitionPropagationSpec
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec::CPartitionPropagationSpec
	(
	CPartIndexMap *ppim,
	CPartFilterMap *ppfm
	)
	:
	m_ppim(ppim),
	m_ppfm(ppfm)
{
	GPOS_ASSERT(NULL != ppim);
	GPOS_ASSERT(NULL != ppfm);
}


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::~CPartitionPropagationSpec
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartitionPropagationSpec::~CPartitionPropagationSpec()
{
	m_ppim->Release();
	m_ppfm->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::UlHash
//
//	@doc:
//		Hash of components
//
//---------------------------------------------------------------------------
ULONG
CPartitionPropagationSpec::UlHash() const
{
	return m_ppim->UlHash();
}


//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::FMatch
//
//	@doc:
//		Check whether two partition propagation specs are equal
//
//---------------------------------------------------------------------------
BOOL
CPartitionPropagationSpec::FMatch
	(
	const CPartitionPropagationSpec *ppps
	) 
	const
{
	return m_ppim->FEqual(ppps->Ppim()) &&
			m_ppfm->FEqual(ppps->m_ppfm);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CPartitionPropagationSpec::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	DrgPexpr *pdrgpexpr, 
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	
	DrgPul *pdrgpul = m_ppim->PdrgpulScanIds(pmp);
	const ULONG ulSize = pdrgpul->UlLength();
	
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		ULONG ulScanId = *((*pdrgpul)[ul]);
		GPOS_ASSERT(m_ppim->FContains(ulScanId));
		
		if (CPartIndexMap::EpimConsumer != m_ppim->Epim(ulScanId) || 0 < m_ppim->UlExpectedPropagators(ulScanId))
		{
			continue;
		}
		
		if (!FRequiresPartitionPropagation(pmp, pexpr, exprhdl, ulScanId))
		{
			continue;
		}
		
		CExpression *pexprResolver = NULL;

		IMDId *pmdid = m_ppim->PmdidRel(ulScanId);
		DrgDrgPcr *pdrgpdrgpcrKeys = NULL;
		DrgPpartkeys *pdrgppartkeys = m_ppim->Pdrgppartkeys(ulScanId);
		CPartConstraint *ppartcnstr = m_ppim->PpartcnstrRel(ulScanId);
		PartCnstrMap *ppartcnstrmap = m_ppim->Ppartcnstrmap(ulScanId);
		pmdid->AddRef();
		ppartcnstr->AddRef();
		ppartcnstrmap->AddRef();
		pexpr->AddRef();
		
		// check if there is a predicate on this part index id
		HMUlExpr *phmulexprEqFilter = GPOS_NEW(pmp) HMUlExpr(pmp);
		HMUlExpr *phmulexprFilter = GPOS_NEW(pmp) HMUlExpr(pmp);
		CExpression *pexprResidual = NULL;
		if (m_ppfm->FContainsScanId(ulScanId))
		{
			CExpression *pexprScalar = PexprFilter(pmp, ulScanId);
			
			// find out which keys are used in the predicate, in case there are multiple
			// keys at this point (e.g. from a union of multiple CTE consumers)
			CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();
			const ULONG ulKeysets = pdrgppartkeys->UlLength();
			for (ULONG ulKey = 0; NULL == pdrgpdrgpcrKeys && ulKey < ulKeysets; ulKey++)
			{
				// get partition key
				CPartKeys *ppartkeys = (*pdrgppartkeys)[ulKey];
				if (ppartkeys->FOverlap(pcrsUsed))
				{
					pdrgpdrgpcrKeys = ppartkeys->Pdrgpdrgpcr();
				}
			}
			
                        // if we cannot find partition keys mapping the partition predicates, fall back to planner
                        if (NULL == pdrgpdrgpcrKeys)
                        {
                            GPOS_RAISE(gpopt::ExmaGPOPT, gpopt::ExmiUnsatisfiedRequiredProperties);
                        }

			pdrgpdrgpcrKeys->AddRef();

			// split predicates and put them in the appropriate hashmaps
			SplitPartPredicates(pmp, pexprScalar, pdrgpdrgpcrKeys, phmulexprEqFilter, phmulexprFilter, &pexprResidual);
			pexprScalar->Release();
		}
		else
		{
			// doesn't matter which keys we use here since there is no filter
			GPOS_ASSERT(1 <= pdrgppartkeys->UlLength());
			pdrgpdrgpcrKeys = (*pdrgppartkeys)[0]->Pdrgpdrgpcr();
			pdrgpdrgpcrKeys->AddRef();
		}

		pexprResolver = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CPhysicalPartitionSelector
												(
												pmp,
												ulScanId,
												pmdid,
												pdrgpdrgpcrKeys,
												ppartcnstrmap,
												ppartcnstr,
												phmulexprEqFilter,
												phmulexprFilter,
												pexprResidual
												),
									pexpr
									);
		
		pdrgpexpr->Append(pexprResolver);
	}
	pdrgpul->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PexprFilter
//
//	@doc:
//		Return the filter expression for the given Scan Id
//
//---------------------------------------------------------------------------
CExpression *
CPartitionPropagationSpec::PexprFilter
	(
	IMemoryPool *pmp,
	ULONG ulScanId
	)
{
	CExpression *pexprScalar = m_ppfm->Pexpr(ulScanId);
	GPOS_ASSERT(NULL != pexprScalar);

	if (CUtils::FScalarIdent(pexprScalar))
	{
		// condition of the form "pkey": translate into pkey = true
		pexprScalar->AddRef();
		pexprScalar = CUtils::PexprScalarEqCmp(pmp, pexprScalar, CUtils::PexprScalarConstBool(pmp, true /*fVal*/, false /*fNull*/));
	}
	else if (CPredicateUtils::FNot(pexprScalar) && CUtils::FScalarIdent((*pexprScalar)[0]))
	{
		// condition of the form "!pkey": translate into pkey = false
		CExpression *pexprId = (*pexprScalar)[0];
		pexprId->AddRef();

		pexprScalar = CUtils::PexprScalarEqCmp(pmp, pexprId, CUtils::PexprScalarConstBool(pmp, false /*fVal*/, false /*fNull*/));
	}
	else
	{
		pexprScalar->AddRef();
	}

	return pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::FRequiresPartitionPropagation
//
//	@doc:
//		Check if given part index id needs to be enforced on top of the given 
//		expression
//
//---------------------------------------------------------------------------
BOOL
CPartitionPropagationSpec::FRequiresPartitionPropagation
	(
	IMemoryPool *pmp, 
	CExpression *pexpr, 
	CExpressionHandle &exprhdl,
	ULONG ulPartIndexId
	)
	const
{
	GPOS_ASSERT(m_ppim->FContains(ulPartIndexId));
	
	// construct partition propagation spec with the given id only, and check if it needs to be 
	// enforced on top
	CPartIndexMap *ppim = GPOS_NEW(pmp) CPartIndexMap(pmp);
	
	IMDId *pmdid = m_ppim->PmdidRel(ulPartIndexId);
	DrgPpartkeys *pdrgppartkeys = m_ppim->Pdrgppartkeys(ulPartIndexId);
	CPartConstraint *ppartcnstr = m_ppim->PpartcnstrRel(ulPartIndexId);
	PartCnstrMap *ppartcnstrmap = m_ppim->Ppartcnstrmap(ulPartIndexId);
	pmdid->AddRef();
	pdrgppartkeys->AddRef();
	ppartcnstr->AddRef();
	ppartcnstrmap->AddRef();
	
	ppim->Insert(ulPartIndexId, ppartcnstrmap, m_ppim->Epim(ulPartIndexId), m_ppim->UlExpectedPropagators(ulPartIndexId), pmdid, pdrgppartkeys, ppartcnstr);
	
	CPartitionPropagationSpec *ppps = GPOS_NEW(pmp) CPartitionPropagationSpec(ppim, GPOS_NEW(pmp) CPartFilterMap(pmp));
	
	CEnfdPartitionPropagation *pepp = GPOS_NEW(pmp) CEnfdPartitionPropagation(ppps, CEnfdPartitionPropagation::EppmSatisfy, GPOS_NEW(pmp) CPartFilterMap(pmp));
	CEnfdProp::EPropEnforcingType epetPartitionPropagation = pepp->Epet(exprhdl, CPhysical::PopConvert(pexpr->Pop()), true /*fPartitionPropagationRequired*/);
	
	pepp->Release();
	
	return CEnfdProp::FEnforce(epetPartitionPropagation);
}

//---------------------------------------------------------------------------
//      @function:
//		CPartitionPropagationSpec::SplitPartPredicates
//
//	@doc:
//		Split the partition elimination predicates over the various levels
//		as well as the residual predicate and add them to the appropriate
//		hashmaps. These are to be used when creating the partition selector
//
//---------------------------------------------------------------------------
void
CPartitionPropagationSpec::SplitPartPredicates
	(
	IMemoryPool *pmp,
	CExpression *pexprScalar,
	DrgDrgPcr *pdrgpdrgpcrKeys,
	HMUlExpr *phmulexprEqFilter,	// output
	HMUlExpr *phmulexprFilter,		// output
	CExpression **ppexprResidual	// output
	)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pdrgpdrgpcrKeys);
	GPOS_ASSERT(NULL != phmulexprEqFilter);
	GPOS_ASSERT(NULL != phmulexprFilter);
	GPOS_ASSERT(NULL != ppexprResidual);
	GPOS_ASSERT(NULL == *ppexprResidual);

	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	CBitSet *pbsUsed = GPOS_NEW(pmp) CBitSet(pmp);
	CColRefSet *pcrsKeys = PcrsKeys(pmp, pdrgpdrgpcrKeys);

	const ULONG ulLevels = pdrgpdrgpcrKeys->UlLength();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *pcr = CUtils::PcrExtractPartKey(pdrgpdrgpcrKeys, ul);
		// find conjuncts for this key and mark their positions
		DrgPexpr *pdrgpexprKey = PdrgpexprPredicatesOnKey(pmp, pdrgpexprConjuncts, pcr, pcrsKeys, &pbsUsed);
		const ULONG ulLen = pdrgpexprKey->UlLength();
		if (ulLen == 0)
		{
			// no predicates on this key
			pdrgpexprKey->Release();
			continue;
		}

		if (ulLen == 1 && CPredicateUtils::FIdentCompare((*pdrgpexprKey)[0], IMDType::EcmptEq, pcr))
		{
			// EqFilters
			// one equality predicate (key = expr); take out the expression
			// and add it to the equality filters map
			CExpression *pexprPartKey = NULL;
			CExpression *pexprOther = NULL;
			IMDType::ECmpType ecmpt = IMDType::EcmptOther;

			CPredicateUtils::ExtractComponents((*pdrgpexprKey)[0], pcr, &pexprPartKey, &pexprOther, &ecmpt);
			GPOS_ASSERT(NULL != pexprOther);
			pexprOther->AddRef();
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulexprEqFilter->FInsert(GPOS_NEW(pmp) ULONG(ul), pexprOther);
			GPOS_ASSERT(fResult);
			pdrgpexprKey->Release();
		}
		else
		{
			// Filters
			// more than one predicate on this key or one non-simple-equality predicate
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulexprFilter->FInsert(GPOS_NEW(pmp) ULONG(ul), CPredicateUtils::PexprConjunction(pmp, pdrgpexprKey));
			GPOS_ASSERT(fResult);
			continue;
		}

	}

	(*ppexprResidual) = PexprResidualFilter(pmp, pdrgpexprConjuncts, pbsUsed);

	pcrsKeys->Release();
	pdrgpexprConjuncts->Release();
	pbsUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PcrsKeys
//
//	@doc:
//		Return a colrefset containing all the part keys
//
//---------------------------------------------------------------------------
CColRefSet *
CPartitionPropagationSpec::PcrsKeys
	(
	IMemoryPool *pmp,
	DrgDrgPcr *pdrgpdrgpcrKeys
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);

	const ULONG ulLevels = pdrgpdrgpcrKeys->UlLength();
	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CColRef *pcr = CUtils::PcrExtractPartKey(pdrgpdrgpcrKeys, ul);
		pcrs->Include(pcr);
	}

	return pcrs;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PexprResidualFilter
//
//	@doc:
//		Return a residual filter given an array of predicates and a bitset
//		indicating which predicates have already been used
//
//---------------------------------------------------------------------------
CExpression *
CPartitionPropagationSpec::PexprResidualFilter
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr,
	CBitSet *pbsUsed
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pbsUsed);

	DrgPexpr *pdrgpexprUnused = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulLen = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		if (pbsUsed->FBit(ul))
		{
			// predicate already considered
			continue;
		}

		CExpression *pexpr = (*pdrgpexpr)[ul];
		pexpr->AddRef();
		pdrgpexprUnused->Append(pexpr);
	}

	CExpression *pexprResult = CPredicateUtils::PexprConjunction(pmp, pdrgpexprUnused);
	if (CUtils::FScalarConstTrue(pexprResult))
	{
		pexprResult->Release();
		pexprResult = NULL;
	}

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::PdrgpexprPredicatesOnKey
//
//	@doc:
//		Returns an array of predicates on the given partitioning key given
//		an array of predicates on all keys
//
//---------------------------------------------------------------------------
DrgPexpr *
CPartitionPropagationSpec::PdrgpexprPredicatesOnKey
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr,
	CColRef *pcr,
	CColRefSet *pcrsKeys,
	CBitSet **ppbs
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(NULL != ppbs);
	GPOS_ASSERT(NULL != *ppbs);

	DrgPexpr *pdrgpexprResult = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulLen = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		if ((*ppbs)->FBit(ul))
		{
			// this expression has already been added for another column
			continue;
		}

		CExpression *pexpr = (*pdrgpexpr)[ul];
		GPOS_ASSERT(pexpr->Pop()->FScalar());

		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed();
		CColRefSet *pcrsUsedKeys = GPOS_NEW(pmp) CColRefSet(pmp, *pcrsUsed);
		pcrsUsedKeys->Intersection(pcrsKeys);

		if (1 == pcrsUsedKeys->CElements() && pcrsUsedKeys->FMember(pcr))
		{
			pexpr->AddRef();
			pdrgpexprResult->Append(pexpr);
			(*ppbs)->FExchangeSet(ul);
		}

		pcrsUsedKeys->Release();
	}

	return pdrgpexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartitionPropagationSpec::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CPartitionPropagationSpec::OsPrint
	(
	IOstream &os
	)
	const
{
	os << *m_ppim;
	
	os << "Filters: [";
	m_ppfm->OsPrint(os);
	os << "]";
	return os;
}


// EOF

