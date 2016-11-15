//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecHashed.cpp
//
//	@doc:
//		Specification of hashed distribution
//---------------------------------------------------------------------------

#include "naucrates/traceflags/traceflags.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/operators/CPhysicalMotionBroadcast.h"
#include "gpopt/operators/CPhysicalMotionHashDistribute.h"
#include "gpopt/operators/CScalarIdent.h"

#define GPOPT_DISTR_SPEC_HASHED_EXPRESSIONS      (ULONG(5))

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::CDistributionSpecHashed
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecHashed::CDistributionSpecHashed
	(
	DrgPexpr *pdrgpexpr,
	BOOL fNullsColocated
	)
	:
	m_pdrgpexpr(pdrgpexpr),
	m_fNullsColocated(fNullsColocated),
	m_pdshashedEquiv(NULL)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->UlLength());
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::CDistributionSpecHashed
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDistributionSpecHashed::CDistributionSpecHashed
	(
	DrgPexpr *pdrgpexpr,
	BOOL fNullsColocated,
	CDistributionSpecHashed *pdshashedEquiv
	)
	:
	m_pdrgpexpr(pdrgpexpr),
	m_fNullsColocated(fNullsColocated),
	m_pdshashedEquiv(pdshashedEquiv)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pdshashedEquiv);
	GPOS_ASSERT(0 < pdrgpexpr->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::~CDistributionSpecHashed
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDistributionSpecHashed::~CDistributionSpecHashed()
{
	m_pdrgpexpr->Release();
	CRefCount::SafeRelease(m_pdshashedEquiv);
}

//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PdsCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the distribution spec with remapped columns
//
//---------------------------------------------------------------------------
CDistributionSpec *
CDistributionSpecHashed::PdsCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulLen = m_pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		pdrgpexpr->Append(pexpr->PexprCopyWithRemappedColumns(pmp, phmulcr, fMustExist));
	}

	if (NULL == m_pdshashedEquiv)
	{
		return GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, m_fNullsColocated);
	}

	// copy equivalent distribution
	CDistributionSpec *pds = m_pdshashedEquiv->PdsCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
	CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
	return GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, m_fNullsColocated, pdshashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FSatisfies
//
//	@doc:
//		Check if this distribution spec satisfies the given one
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FSatisfies
	(
	const CDistributionSpec *pds
	)
	const
{	
	if (NULL != m_pdshashedEquiv && m_pdshashedEquiv->FSatisfies(pds))
 	{
 		return true;
 	}

	if (FMatch(pds))
	{
		// exact match implies satisfaction
		return true;
	 }

	if (EdtAny == pds->Edt() || EdtNonSingleton == pds->Edt())
	{
		// hashed distribution satisfies the "any" and "non-singleton" distribution requirement
		return true;
	}

	if (EdtHashed != pds->Edt())
	{
		return false;
	}
	
	GPOS_ASSERT(EdtHashed == pds->Edt());
	
	const CDistributionSpecHashed *pdsHashed =
			dynamic_cast<const CDistributionSpecHashed *>(pds);

	return FMatchSubset(pdsHashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FMatchSubset
//
//	@doc:
//		Check if the expressions of the object match a subset of the passed spec's
//		expressions
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FMatchSubset
	(
	const CDistributionSpecHashed *pdsHashed
	)
	const
{
	const ULONG ulOwnExprs = m_pdrgpexpr->UlLength();
	const ULONG ulOtherExprs = pdsHashed->m_pdrgpexpr->UlLength();

	if (ulOtherExprs < ulOwnExprs || !FNullsColocated(pdsHashed) || !FDuplicateSensitiveCompatible(pdsHashed))
	{
		return false;
	}

	for (ULONG ulOuter = 0; ulOuter < ulOwnExprs; ulOuter++)
	{
		CExpression *pexprOwn = CUtils::PexprWithoutBinaryCoercibleCasts((*m_pdrgpexpr)[ulOuter]);

		BOOL fFound = false;
		for (ULONG ulInner = 0; ulInner < ulOtherExprs; ulInner++)
		{
			CExpression *pexprOther = CUtils::PexprWithoutBinaryCoercibleCasts((*(pdsHashed->m_pdrgpexpr))[ulInner]);
			if (CUtils::FEqual(pexprOwn, pexprOther))
			{
				fFound = true;
				break;
			}
		}

		if (!fFound)
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PdshashedExcludeColumns
//
//	@doc:
//		Return a copy of the distribution spec after excluding the given
//		columns, return NULL if all distribution expressions are excluded
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CDistributionSpecHashed::PdshashedExcludeColumns
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pcrs);

	DrgPexpr *pdrgpexprNew = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulExprs = m_pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulExprs; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		COperator *pop = pexpr->Pop();
		if (COperator::EopScalarIdent == pop->Eopid())
		{
			// we only care here about column identifiers,
			// any more complicated expressions are copied to output
			const CColRef *pcr = CScalarIdent::PopConvert(pop)->Pcr();
			if (pcrs->FMember(pcr))
			{
				continue;
			}
		}

		pexpr->AddRef();
		pdrgpexprNew->Append(pexpr);
	}

	if (0 == pdrgpexprNew->UlLength())
	{
		pdrgpexprNew->Release();
		return NULL;
	}

	return GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexprNew, m_fNullsColocated);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FEqual
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FEqual
	(
	const CDistributionSpecHashed *pdshashed
	)
	const
{
	return m_fNullsColocated == pdshashed->FNullsColocated() &&
			m_fDuplicateSensitive == pdshashed->FDuplicateSensitive() &&
			m_fSatisfiedBySingleton == pdshashed->FSatisfiedBySingleton() &&
			CUtils::FEqual(m_pdrgpexpr, pdshashed->m_pdrgpexpr) &&
			Edt() == pdshashed->Edt();
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::AppendEnforcers
//
//	@doc:
//		Add required enforcers to dynamic array
//
//---------------------------------------------------------------------------
void
CDistributionSpecHashed::AppendEnforcers
	(
	IMemoryPool *pmp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	DrgPexpr *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != prpp);
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(!GPOS_FTRACE(EopttraceDisableMotions));
	GPOS_ASSERT(this == prpp->Ped()->PdsRequired() &&
	            "required plan properties don't match enforced distribution spec");

	if (GPOS_FTRACE(EopttraceDisableMotionHashDistribute))
	{
		// hash-distribute Motion is disabled
		return;
	}

	// add a hashed distribution enforcer
	AddRef();
	pexpr->AddRef();
	CExpression *pexprMotion = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CPhysicalMotionHashDistribute(pmp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG 
CDistributionSpecHashed::UlHash() const
{
	ULONG ulHash = (ULONG) Edt();
	ULONG ulHashedExpressions = std::min(m_pdrgpexpr->UlLength(), GPOPT_DISTR_SPEC_HASHED_EXPRESSIONS);
	
	for (ULONG ul = 0; ul < ulHashedExpressions; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		ulHash = gpos::UlCombineHashes(ulHash, CExpression::UlHash(pexpr));
	}

	return ulHash;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PcrsUsed
//
//	@doc:
//		Extract columns used by the distribution spec
//
//---------------------------------------------------------------------------
CColRefSet *
CDistributionSpecHashed::PcrsUsed
	(
	IMemoryPool *pmp
	)
	const
{
	return CUtils::PcrsExtractColumns(pmp, m_pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FMatchHashedDistribution
//
//	@doc:
//		Exact match against given hashed distribution
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::FMatchHashedDistribution
	(
	const CDistributionSpecHashed *pdshashed
	)
	const
{
	GPOS_ASSERT(NULL != pdshashed);

	if (m_pdrgpexpr->UlLength() != pdshashed->m_pdrgpexpr->UlLength() ||
		FNullsColocated() != pdshashed->FNullsColocated() ||
		FDuplicateSensitive() != pdshashed->FDuplicateSensitive())
	{
		return false;
	}

	const ULONG ulLen = m_pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		if (!CUtils::FEqual(CUtils::PexprWithoutBinaryCoercibleCasts((*(pdshashed->m_pdrgpexpr))[ul]),
							CUtils::PexprWithoutBinaryCoercibleCasts((*m_pdrgpexpr)[ul])))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::FMatch
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecHashed::FMatch
	(
	const CDistributionSpec *pds
	) 
	const
{
	if (Edt() != pds->Edt())
	{
		return false;
	}

	const CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);

	if (NULL != m_pdshashedEquiv && m_pdshashedEquiv->FMatch(pdshashed))
	{
		return true;
	 }

	if (NULL != pdshashed->PdshashedEquiv() && pdshashed->PdshashedEquiv()->FMatch(this))
	{
		return true;
	}

	return FMatchHashedDistribution(pdshashed);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::PdshashedMaximal
//
//	@doc:
//		Return a hashed distribution on the maximal hashable subset of
//		given columns,
//		if all columns are not hashable, return NULL
//
//---------------------------------------------------------------------------
CDistributionSpecHashed *
CDistributionSpecHashed::PdshashedMaximal
	(
	IMemoryPool *pmp,
	DrgPcr *pdrgpcr,
	BOOL fNullsColocated
	)
{
	GPOS_ASSERT(NULL != pdrgpcr);
	GPOS_ASSERT(0 < pdrgpcr->UlLength());

	DrgPcr *pdrgpcrHashable = CUtils::PdrgpcrHashableSubset(pmp, pdrgpcr);
	CDistributionSpecHashed *pdshashed = NULL;
	if (0 < pdrgpcrHashable->UlLength())
	{
		DrgPexpr *pdrgpexpr = CUtils::PdrgpexprScalarIdents(pmp, pdrgpcrHashable);
		pdshashed = GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, fNullsColocated);
	}
	pdrgpcrHashable->Release();

	return pdshashed;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::OsPrint
//
//	@doc:
//		Print function
//
//---------------------------------------------------------------------------
IOstream &
CDistributionSpecHashed::OsPrint
	(
	IOstream &os
	)
	const
{
	os << this->SzId() << ": [ ";
	const ULONG ulLen = m_pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		os << *((*m_pdrgpexpr)[ul]) << " ";
	}
	if (m_fNullsColocated)
	{
		os <<  ", nulls colocated";
	}
	else
	{
		os <<  ", nulls not colocated";
	}

	if (m_fDuplicateSensitive)
	{
		os <<  ", duplicate sensitive";
	}
	
	if (!m_fSatisfiedBySingleton)
	{
		os << ", across-segments";
	}

	os <<  " ]";

	if (NULL != m_pdshashedEquiv)
	{
		os << ", equiv. dist: ";
		m_pdshashedEquiv->OsPrint(os);
	}

	return os;
}

// EOF

