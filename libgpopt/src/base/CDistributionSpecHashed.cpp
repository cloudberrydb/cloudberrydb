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
#include "gpopt/base/CCastUtils.h"
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
	CExpressionArray *pdrgpexpr,
	BOOL fNullsColocated
	)
	:
	m_pdrgpexpr(pdrgpexpr),
	m_fNullsColocated(fNullsColocated),
	m_pdshashedEquiv(NULL)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(0 < pdrgpexpr->Size());
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
	CExpressionArray *pdrgpexpr,
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
	GPOS_ASSERT(0 < pdrgpexpr->Size());
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
	IMemoryPool *mp,
	UlongToColRefMap *colref_mapping,
	BOOL must_exist
	)
{
	CExpressionArray *pdrgpexpr = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG length = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		pdrgpexpr->Append(pexpr->PexprCopyWithRemappedColumns(mp, colref_mapping, must_exist));
	}

	if (NULL == m_pdshashedEquiv)
	{
		return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, m_fNullsColocated);
	}

	// copy equivalent distribution
	CDistributionSpec *pds = m_pdshashedEquiv->PdsCopyWithRemappedColumns(mp, colref_mapping, must_exist);
	CDistributionSpecHashed *pdshashed = CDistributionSpecHashed::PdsConvert(pds);
	return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, m_fNullsColocated, pdshashed);
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

	if (Matches(pds))
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
	const ULONG ulOwnExprs = m_pdrgpexpr->Size();
	const ULONG ulOtherExprs = pdsHashed->m_pdrgpexpr->Size();

	if (ulOtherExprs < ulOwnExprs || !FNullsColocated(pdsHashed) || !FDuplicateSensitiveCompatible(pdsHashed))
	{
		return false;
	}

	for (ULONG ulOuter = 0; ulOuter < ulOwnExprs; ulOuter++)
	{
		CExpression *pexprOwn = CCastUtils::PexprWithoutBinaryCoercibleCasts((*m_pdrgpexpr)[ulOuter]);

		BOOL fFound = false;
		for (ULONG ulInner = 0; ulInner < ulOtherExprs; ulInner++)
		{
			CExpression *pexprOther = CCastUtils::PexprWithoutBinaryCoercibleCasts((*(pdsHashed->m_pdrgpexpr))[ulInner]);
			if (CUtils::Equals(pexprOwn, pexprOther))
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
	IMemoryPool *mp,
	CColRefSet *pcrs
	)
{
	GPOS_ASSERT(NULL != pcrs);

	CExpressionArray *pdrgpexprNew = GPOS_NEW(mp) CExpressionArray(mp);
	const ULONG ulExprs = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < ulExprs; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		COperator *pop = pexpr->Pop();
		if (COperator::EopScalarIdent == pop->Eopid())
		{
			// we only care here about column identifiers,
			// any more complicated expressions are copied to output
			const CColRef *colref = CScalarIdent::PopConvert(pop)->Pcr();
			if (pcrs->FMember(colref))
			{
				continue;
			}
		}

		pexpr->AddRef();
		pdrgpexprNew->Append(pexpr);
	}

	if (0 == pdrgpexprNew->Size())
	{
		pdrgpexprNew->Release();
		return NULL;
	}

	return GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexprNew, m_fNullsColocated);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::Equals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CDistributionSpecHashed::Equals
	(
	const CDistributionSpecHashed *pdshashed
	)
	const
{
	return m_fNullsColocated == pdshashed->FNullsColocated() &&
			m_is_duplicate_sensitive == pdshashed->IsDuplicateSensitive() &&
			m_fSatisfiedBySingleton == pdshashed->FSatisfiedBySingleton() &&
			CUtils::Equals(m_pdrgpexpr, pdshashed->m_pdrgpexpr) &&
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
	IMemoryPool *mp,
	CExpressionHandle &, // exprhdl
	CReqdPropPlan *
#ifdef GPOS_DEBUG
	prpp
#endif // GPOS_DEBUG
	,
	CExpressionArray *pdrgpexpr,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != mp);
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
	CExpression *pexprMotion = GPOS_NEW(mp) CExpression
										(
										mp,
										GPOS_NEW(mp) CPhysicalMotionHashDistribute(mp, this),
										pexpr
										);
	pdrgpexpr->Append(pexprMotion);
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::HashValue
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG 
CDistributionSpecHashed::HashValue() const
{
	ULONG ulHash = (ULONG) Edt();
	ULONG ulHashedExpressions = std::min(m_pdrgpexpr->Size(), GPOPT_DISTR_SPEC_HASHED_EXPRESSIONS);
	
	for (ULONG ul = 0; ul < ulHashedExpressions; ul++)
	{
		CExpression *pexpr = (*m_pdrgpexpr)[ul];
		ulHash = gpos::CombineHashes(ulHash, CExpression::HashValue(pexpr));
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
	IMemoryPool *mp
	)
	const
{
	return CUtils::PcrsExtractColumns(mp, m_pdrgpexpr);
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

	if (m_pdrgpexpr->Size() != pdshashed->m_pdrgpexpr->Size() ||
		FNullsColocated() != pdshashed->FNullsColocated() ||
		IsDuplicateSensitive() != pdshashed->IsDuplicateSensitive())
	{
		return false;
	}

	const ULONG length = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
	{
		if (!CUtils::Equals(CCastUtils::PexprWithoutBinaryCoercibleCasts((*(pdshashed->m_pdrgpexpr))[ul]),
							CCastUtils::PexprWithoutBinaryCoercibleCasts((*m_pdrgpexpr)[ul])))
		{
			return false;
		}
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CDistributionSpecHashed::Matches
//
//	@doc:
//		Match function
//
//---------------------------------------------------------------------------
BOOL 
CDistributionSpecHashed::Matches
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

	if (NULL != m_pdshashedEquiv && m_pdshashedEquiv->Matches(pdshashed))
	{
		return true;
	 }

	if (NULL != pdshashed->PdshashedEquiv() && pdshashed->PdshashedEquiv()->Matches(this))
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
	IMemoryPool *mp,
	CColRefArray *colref_array,
	BOOL fNullsColocated
	)
{
	GPOS_ASSERT(NULL != colref_array);
	GPOS_ASSERT(0 < colref_array->Size());

	CColRefArray *pdrgpcrHashable = CUtils::PdrgpcrRedistributableSubset(mp, colref_array);
	CDistributionSpecHashed *pdshashed = NULL;
	if (0 < pdrgpcrHashable->Size())
	{
		CExpressionArray *pdrgpexpr = CUtils::PdrgpexprScalarIdents(mp, pdrgpcrHashable);
		pdshashed = GPOS_NEW(mp) CDistributionSpecHashed(pdrgpexpr, fNullsColocated);
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
	const ULONG length = m_pdrgpexpr->Size();
	for (ULONG ul = 0; ul < length; ul++)
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

	if (m_is_duplicate_sensitive)
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

