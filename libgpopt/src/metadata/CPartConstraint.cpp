//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPartConstraint.cpp
//
//	@doc:
//		Implementation of part constraints
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CConstraintNegation.h"
#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/CUtils.h"

#include "gpopt/metadata/CPartConstraint.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CPartConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartConstraint::CPartConstraint
	(
	IMemoryPool *pmp,
	HMUlCnstr *phmulcnstr,
	CBitSet *pbsDefaultParts,
	BOOL fUnbounded,
	DrgDrgPcr *pdrgpdrgpcr
	)
	:
	m_phmulcnstr(phmulcnstr),
	m_pbsDefaultParts(pbsDefaultParts),
	m_fUnbounded(fUnbounded),
	m_fUninterpreted(false),
	m_pdrgpdrgpcr(pdrgpdrgpcr)
{
	GPOS_ASSERT(NULL != phmulcnstr);
	GPOS_ASSERT(NULL != pbsDefaultParts);
	GPOS_ASSERT(NULL != pdrgpdrgpcr);
	m_ulLevels = pdrgpdrgpcr->UlLength();
	GPOS_ASSERT_IMP(fUnbounded, FAllDefaultPartsIncluded());

	m_pcnstrCombined = PcnstrBuildCombined(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CPartConstraint
//
//	@doc:
//		Ctor - shortcut for single-level
//
//---------------------------------------------------------------------------
CPartConstraint::CPartConstraint
	(
	IMemoryPool *pmp,
	CConstraint *pcnstr,
	BOOL fDefaultPartition,
	BOOL fUnbounded
	)
	:
	m_phmulcnstr(NULL),
	m_pbsDefaultParts(NULL),
	m_fUnbounded(fUnbounded),
	m_fUninterpreted(false)
{
	GPOS_ASSERT(NULL != pcnstr);
	GPOS_ASSERT_IMP(fUnbounded, fDefaultPartition);

	m_phmulcnstr = GPOS_NEW(pmp) HMUlCnstr(pmp);
#ifdef GPOS_DEBUG
	BOOL fResult =
#endif // GPOS_DEBUG
	m_phmulcnstr->FInsert(GPOS_NEW(pmp) ULONG(0 /*ulLevel*/), pcnstr);
	GPOS_ASSERT(fResult);

	CColRefSet *pcrsUsed = pcnstr->PcrsUsed();
	GPOS_ASSERT(1 == pcrsUsed->CElements());
	CColRef *pcrPartKey = pcrsUsed->PcrFirst();

	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcr->Append(pcrPartKey);

	m_pdrgpdrgpcr = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	m_pdrgpdrgpcr->Append(pdrgpcr);

	m_ulLevels = 1;
	m_pbsDefaultParts = GPOS_NEW(pmp) CBitSet(pmp);
	if (fDefaultPartition)
	{
		m_pbsDefaultParts->FExchangeSet(0 /*ulBit*/);
	}

	pcnstr->AddRef();
	m_pcnstrCombined = pcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CPartConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CPartConstraint::CPartConstraint
	(
	BOOL fUninterpreted
	)
	:
	m_phmulcnstr(NULL),
	m_pbsDefaultParts(NULL),
	m_ulLevels(1),
	m_fUnbounded(false),
	m_fUninterpreted(fUninterpreted),
	m_pdrgpdrgpcr(NULL),
	m_pcnstrCombined(NULL)
{
	GPOS_ASSERT(fUninterpreted);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::~CPartConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CPartConstraint::~CPartConstraint()
{
	CRefCount::SafeRelease(m_phmulcnstr);
	CRefCount::SafeRelease(m_pbsDefaultParts);
	CRefCount::SafeRelease(m_pdrgpdrgpcr);
	CRefCount::SafeRelease(m_pcnstrCombined);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PcnstrBuildCombined
//
//	@doc:
//		Construct the combined constraint
//
//---------------------------------------------------------------------------
CConstraint *
CPartConstraint::PcnstrBuildCombined
	(
	IMemoryPool *pmp
	)
{
	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		CConstraint *pcnstr = m_phmulcnstr->PtLookup(&ul);
		if (NULL != pcnstr)
		{
			pcnstr->AddRef();
			pdrgpcnstr->Append(pcnstr);
		}
	}

	return CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FAllDefaultPartsIncluded
//
//	@doc:
//		Are all default partitions on all levels included
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FAllDefaultPartsIncluded()
{
	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		if (!FDefaultPartition(ul))
		{
			return false;
		}
	}

	return true;
}
#endif //GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FUnbounded
//
//	@doc:
//		Is part constraint unbounded
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FUnbounded() const
{
	return m_fUnbounded;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FEquivalent
//
//	@doc:
//		Are constraints equivalent
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FEquivalent
	(
	const CPartConstraint *ppartcnstr
	)
	const
{
	GPOS_ASSERT(NULL != ppartcnstr);

	if (m_fUninterpreted || ppartcnstr->FUninterpreted())
	{
		return m_fUninterpreted && ppartcnstr->FUninterpreted();
	}

	if (FUnbounded())
	{
		return ppartcnstr->FUnbounded();
	}
	
	return m_ulLevels == ppartcnstr->m_ulLevels &&
			m_pbsDefaultParts->FEqual(ppartcnstr->m_pbsDefaultParts) &&
			FEqualConstrMaps(m_phmulcnstr, ppartcnstr->m_phmulcnstr, m_ulLevels);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FEqualConstrMaps
//
//	@doc:
//		Check if two constaint maps have the same constraints
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FEqualConstrMaps
	(
	HMUlCnstr *phmulcnstrFst,
	HMUlCnstr *phmulcnstrSnd,
	ULONG ulLevels
	)
{
	if (phmulcnstrFst->UlEntries() != phmulcnstrSnd->UlEntries())
	{
		return false;
	}

	for (ULONG ul = 0; ul < ulLevels; ul++)
	{
		CConstraint *pcnstrFst = phmulcnstrFst->PtLookup(&ul);
		CConstraint *pcnstrSnd = phmulcnstrSnd->PtLookup(&ul);

		if ((NULL == pcnstrFst || NULL == pcnstrSnd) && pcnstrFst != pcnstrSnd)
		{
			return false;
		}

		if (NULL != pcnstrFst && !pcnstrFst->FEquals(pcnstrSnd))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::Pcnstr
//
//	@doc:
//		Constraint at given level
//
//---------------------------------------------------------------------------
CConstraint *
CPartConstraint::Pcnstr
	(
	ULONG ulLevel
	)
	const
{
	GPOS_ASSERT(!m_fUninterpreted && "Calling Pcnstr on uninterpreted partition constraint");
	return m_phmulcnstr->PtLookup(&ulLevel);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FOverlapLevel
//
//	@doc:
//		Does the current constraint overlap with given one at the given level
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FOverlapLevel
	(
	IMemoryPool *pmp,
	const CPartConstraint *ppartcnstr,
	ULONG ulLevel
	)
	const
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(!FUnbounded());
	GPOS_ASSERT(!ppartcnstr->FUnbounded());

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	CConstraint *pcnstrCurrent = Pcnstr(ulLevel);
	CConstraint *pcnstrOther = ppartcnstr->Pcnstr(ulLevel);
	GPOS_ASSERT(NULL != pcnstrCurrent);
	GPOS_ASSERT(NULL != pcnstrOther);

	pcnstrCurrent->AddRef();
	pcnstrOther->AddRef();
	pdrgpcnstr->Append(pcnstrCurrent);
	pdrgpcnstr->Append(pcnstrOther);

	CConstraint *pcnstrIntersect = CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);

	BOOL fOverlap = !pcnstrIntersect->FContradiction();
	pcnstrIntersect->Release();

	return fOverlap || (FDefaultPartition(ulLevel) && ppartcnstr->FDefaultPartition(ulLevel));
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FOverlap
//
//	@doc:
//		Does constraint overlap with given one
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FOverlap
	(
	IMemoryPool *pmp,
	const CPartConstraint *ppartcnstr
	)
	const
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(!m_fUninterpreted && "Calling FOverlap on uninterpreted partition constraint");
	
	if (FUnbounded() || ppartcnstr->FUnbounded())
	{
		return true;
	}
	
	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		if (!FOverlapLevel(pmp, ppartcnstr, ul))
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FSubsume
//
//	@doc:
//		Does constraint subsume given one
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FSubsume
	(
	const CPartConstraint *ppartcnstr
	)
	const
{
	GPOS_ASSERT(NULL != ppartcnstr);
	GPOS_ASSERT(!m_fUninterpreted && "Calling FSubsume on uninterpreted partition constraint");

	if (FUnbounded())
	{
		return true;
	}

	if (ppartcnstr->FUnbounded())
	{
		return false;
	}

	BOOL fSubsumeLevel = true;
	for (ULONG ul = 0; ul < m_ulLevels && fSubsumeLevel; ul++)
	{
		CConstraint *pcnstrCurrent = Pcnstr(ul);
		CConstraint *pcnstrOther = ppartcnstr->Pcnstr(ul);
		GPOS_ASSERT(NULL != pcnstrCurrent);
		GPOS_ASSERT(NULL != pcnstrOther);

		fSubsumeLevel = pcnstrCurrent->FContains(pcnstrOther) &&
						(FDefaultPartition(ul) || !ppartcnstr->FDefaultPartition(ul));
	}

	return fSubsumeLevel;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FCanNegate
//
//	@doc:
//		Check whether or not the current part constraint can be negated. A part
//		constraint can be negated only if it has constraints on the first level
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FCanNegate() const
{
	// first level cannot be NULL
	if (NULL == Pcnstr(0))
	{
		return false;
	}

	// all levels after the first must be unconstrained
	for (ULONG ul = 1; ul < m_ulLevels; ul++)
	{
		CConstraint *pcnstr = Pcnstr(ul);
		if (NULL == pcnstr || !pcnstr->FUnbounded())
		{
			return false;
		}
	}

	return true;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrRemaining
//
//	@doc:
//		Return what remains of the current part constraint after taking out
//		the given part constraint. Returns NULL is the difference cannot be
//		performed
//
//---------------------------------------------------------------------------
CPartConstraint *
CPartConstraint::PpartcnstrRemaining
	(
	IMemoryPool *pmp,
	CPartConstraint *ppartcnstr
	)
{
	GPOS_ASSERT(!m_fUninterpreted && "Calling PpartcnstrRemaining on uninterpreted partition constraint");
	GPOS_ASSERT(NULL != ppartcnstr);

	if (m_ulLevels != ppartcnstr->m_ulLevels || !ppartcnstr->FCanNegate())
	{
		return NULL;
	}

	HMUlCnstr *phmulcnstr = GPOS_NEW(pmp) HMUlCnstr(pmp);
	CBitSet *pbsDefaultParts = GPOS_NEW(pmp) CBitSet(pmp);

	// constraint on first level
	CConstraint *pcnstrCurrent = Pcnstr(0 /*ulLevel*/);
	CConstraint *pcnstrOther = ppartcnstr->Pcnstr(0 /*ulLevel*/);

	CConstraint *pcnstrRemaining = PcnstrRemaining(pmp, pcnstrCurrent, pcnstrOther);

#ifdef GPOS_DEBUG
	BOOL fResult =
#endif // GPOS_DEBUG
	phmulcnstr->FInsert(GPOS_NEW(pmp) ULONG(0), pcnstrRemaining);
	GPOS_ASSERT(fResult);

	if (FDefaultPartition(0 /*ulLevel*/) && !ppartcnstr->FDefaultPartition(0 /*ulLevel*/))
	{
		pbsDefaultParts->FExchangeSet(0 /*ulBit*/);
	}

	// copy the remaining constraints and default partition flags
	for (ULONG ul = 1; ul < m_ulLevels; ul++)
	{
		CConstraint *pcnstrLevel = Pcnstr(ul);
		if (NULL != pcnstrLevel)
		{
			pcnstrLevel->AddRef();
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulcnstr->FInsert(GPOS_NEW(pmp) ULONG(ul), pcnstrLevel);
			GPOS_ASSERT(fResult);
		}

		if (FDefaultPartition(ul))
		{
			pbsDefaultParts->FExchangeSet(ul);
		}
	}

	m_pdrgpdrgpcr->AddRef();
	return GPOS_NEW(pmp) CPartConstraint(pmp, phmulcnstr, pbsDefaultParts, false /*fUnbounded*/, m_pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PcnstrRemaining
//
//	@doc:
//		Return the remaining part of the first constraint that is not covered by
//		the second constraint
//
//---------------------------------------------------------------------------
CConstraint *
CPartConstraint::PcnstrRemaining
	(
	IMemoryPool *pmp,
	CConstraint *pcnstrFst,
	CConstraint *pcnstrSnd
	)
{
	GPOS_ASSERT(NULL != pcnstrSnd);

	pcnstrSnd->AddRef();
	CConstraint *pcnstrNegation = GPOS_NEW(pmp) CConstraintNegation(pmp, pcnstrSnd);

	if (NULL == pcnstrFst || pcnstrFst->FUnbounded())
	{
		return pcnstrNegation;
	}

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	pcnstrFst->AddRef();
	pdrgpcnstr->Append(pcnstrFst);
	pdrgpcnstr->Append(pcnstrNegation);

	return GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcnstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the part constraint with remapped columns
//
//---------------------------------------------------------------------------
CPartConstraint *
CPartConstraint::PpartcnstrCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	if (m_fUninterpreted)
	{
		return GPOS_NEW(pmp) CPartConstraint(true /*m_fUninterpreted*/);
	}

	HMUlCnstr *phmulcnstr = GPOS_NEW(pmp) HMUlCnstr(pmp);
	DrgDrgPcr *pdrgpdrgpcr = GPOS_NEW(pmp) DrgDrgPcr(pmp);

	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		DrgPcr *pdrgpcr = (*m_pdrgpdrgpcr)[ul];
		DrgPcr *pdrgpcrMapped = CUtils::PdrgpcrRemap(pmp, pdrgpcr, phmulcr, fMustExist);
		pdrgpdrgpcr->Append(pdrgpcrMapped);

		CConstraint *pcnstr = Pcnstr(ul);
		if (NULL != pcnstr)
		{
			CConstraint *pcnstrRemapped = pcnstr->PcnstrCopyWithRemappedColumns(pmp, phmulcr, fMustExist);
#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
			phmulcnstr->FInsert(GPOS_NEW(pmp) ULONG(ul), pcnstrRemapped);
			GPOS_ASSERT(fResult);
		}
	}

	m_pbsDefaultParts->AddRef();
	return GPOS_NEW(pmp) CPartConstraint(pmp, phmulcnstr, m_pbsDefaultParts, m_fUnbounded, pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CPartConstraint::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "Part constraint: (";
	if (m_fUninterpreted)
	{
		os << "uninterpreted)";
		return os;
	}

	for (ULONG ul = 0; ul < m_ulLevels; ul++)
	{
		if (ul > 0)
		{
			os << ", ";
		}
		CConstraint *pcnstr = Pcnstr(ul);
		if (NULL != pcnstr)
		{
			pcnstr->OsPrint(os);
		}
		else
		{
			os << "-";
		}
	}
	
	os << ", default partitions on levels: " << *m_pbsDefaultParts
		<< ", unbounded: " << m_fUnbounded;
	os << ")";
	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::FDisjunctionPossible
//
//	@doc:
//		Check if it is possible to produce a disjunction of the two given part
//		constraints. This is possible if the first ulLevels-1 have the same
//		constraints and default flags for both part constraints
//
//---------------------------------------------------------------------------
BOOL
CPartConstraint::FDisjunctionPossible
	(
	CPartConstraint *ppartcnstrFst,
	CPartConstraint *ppartcnstrSnd
	)
{
	GPOS_ASSERT(NULL != ppartcnstrFst);
	GPOS_ASSERT(NULL != ppartcnstrSnd);
	GPOS_ASSERT(ppartcnstrFst->m_ulLevels == ppartcnstrSnd->m_ulLevels);

	const ULONG ulLevels = ppartcnstrFst->m_ulLevels;
	BOOL fSuccess = true;

	for (ULONG ul = 0; fSuccess && ul < ulLevels - 1; ul++)
	{
		CConstraint *pcnstrFst = ppartcnstrFst->Pcnstr(ul);
		CConstraint *pcnstrSnd = ppartcnstrSnd->Pcnstr(ul);
		fSuccess = (NULL != pcnstrFst &&
					NULL != pcnstrSnd &&
					pcnstrFst->FEquals(pcnstrSnd) &&
					ppartcnstrFst->FDefaultPartition(ul) == ppartcnstrSnd->FDefaultPartition(ul));
	}

	// last level constraints cannot be NULL as well
	fSuccess = (fSuccess &&
				NULL != ppartcnstrFst->Pcnstr(ulLevels - 1) &&
				NULL != ppartcnstrSnd->Pcnstr(ulLevels - 1));

	return fSuccess;
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrDisjunction
//
//	@doc:
//		Construct a disjunction of the two part constraints. We can only
//		construct this disjunction if they differ only on the last level
//
//---------------------------------------------------------------------------
CPartConstraint *
CPartConstraint::PpartcnstrDisjunction
	(
	IMemoryPool *pmp,
	CPartConstraint *ppartcnstrFst,
	CPartConstraint *ppartcnstrSnd
	)
{
	GPOS_ASSERT(NULL != ppartcnstrFst);
	GPOS_ASSERT(NULL != ppartcnstrSnd);

	if (ppartcnstrFst->FUnbounded())
	{
		ppartcnstrFst->AddRef();
		return ppartcnstrFst;
	}
	
	if (ppartcnstrSnd->FUnbounded())
	{
		ppartcnstrSnd->AddRef();
		return ppartcnstrSnd;
	}

	if (!FDisjunctionPossible(ppartcnstrFst, ppartcnstrSnd))
	{
		return NULL;
	}

	HMUlCnstr *phmulcnstr = GPOS_NEW(pmp) HMUlCnstr(pmp);
	CBitSet *pbsCombined = GPOS_NEW(pmp) CBitSet(pmp);

	const ULONG ulLevels = ppartcnstrFst->m_ulLevels;
	for (ULONG ul = 0; ul < ulLevels-1; ul++)
	{
		CConstraint *pcnstrFst = ppartcnstrFst->Pcnstr(ul);

		pcnstrFst->AddRef();
#ifdef GPOS_DEBUG
		BOOL fResult =
#endif // GPOS_DEBUG
		phmulcnstr->FInsert(GPOS_NEW(pmp) ULONG(ul), pcnstrFst);
		GPOS_ASSERT(fResult);

		if (ppartcnstrFst->FDefaultPartition(ul))
		{
			pbsCombined->FExchangeSet(ul);
		}
	}

	// create the disjunction between the constraints of the last level
	CConstraint *pcnstrFst = ppartcnstrFst->Pcnstr(ulLevels - 1);
	CConstraint *pcnstrSnd = ppartcnstrSnd->Pcnstr(ulLevels - 1);

	pcnstrFst->AddRef();
	pcnstrSnd->AddRef();
	DrgPcnstr *pdrgpcnstrCombined = GPOS_NEW(pmp) DrgPcnstr(pmp);
	
	pdrgpcnstrCombined->Append(pcnstrFst);
	pdrgpcnstrCombined->Append(pcnstrSnd);

	CConstraint *pcnstrDisj = CConstraint::PcnstrDisjunction(pmp, pdrgpcnstrCombined);
	GPOS_ASSERT(NULL != pcnstrDisj);
#ifdef GPOS_DEBUG
	BOOL fResult =
#endif // GPOS_DEBUG
	phmulcnstr->FInsert(GPOS_NEW(pmp) ULONG(ulLevels - 1), pcnstrDisj);
	GPOS_ASSERT(fResult);

	if (ppartcnstrFst->FDefaultPartition(ulLevels - 1) || ppartcnstrSnd->FDefaultPartition(ulLevels - 1))
	{
		pbsCombined->FExchangeSet(ulLevels - 1);
	}

	DrgDrgPcr *pdrgpdrgpcr = ppartcnstrFst->Pdrgpdrgpcr();
	pdrgpdrgpcr->AddRef();
	return GPOS_NEW(pmp) CPartConstraint(pmp, phmulcnstr, pbsCombined, false /*fUnbounded*/, pdrgpdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::CopyPartConstraints
//
//	@doc:
//		Copy the part constraints to the given destination part constraint map
//
//---------------------------------------------------------------------------
void
CPartConstraint::CopyPartConstraints
	(
	IMemoryPool *pmp,
	PartCnstrMap *ppartcnstrmapDest,
	PartCnstrMap *ppartcnstrmapSource
	)
{
	GPOS_ASSERT(NULL != ppartcnstrmapDest);
	GPOS_ASSERT(NULL != ppartcnstrmapSource);

	PartCnstrMapIter pcmi(ppartcnstrmapSource);

	while (pcmi.FAdvance())
	{
		ULONG ulKey = *(pcmi.Pk());
		CPartConstraint *ppartcnstrSource = const_cast<CPartConstraint *>(pcmi.Pt());

		CPartConstraint *ppartcnstrDest = ppartcnstrmapDest->PtLookup(&ulKey);
		GPOS_ASSERT_IMP(NULL != ppartcnstrDest, ppartcnstrDest->FEquivalent(ppartcnstrSource));

		if (NULL == ppartcnstrDest)
		{
			ppartcnstrSource->AddRef();

#ifdef GPOS_DEBUG
			BOOL fResult =
#endif // GPOS_DEBUG
				ppartcnstrmapDest->FInsert(GPOS_NEW(pmp) ULONG(ulKey), ppartcnstrSource);

			GPOS_ASSERT(fResult && "Duplicate part constraints");
		}
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CPartConstraint::PpartcnstrmapCombine
//
//	@doc:
//		Combine the two given part constraint maps and return the result
//
//---------------------------------------------------------------------------
PartCnstrMap *
CPartConstraint::PpartcnstrmapCombine
	(
	IMemoryPool *pmp,
	PartCnstrMap *ppartcnstrmapFst,
	PartCnstrMap *ppartcnstrmapSnd
	)
{
	if (NULL == ppartcnstrmapFst && NULL == ppartcnstrmapSnd)
	{
		return NULL;
	}

	if (NULL == ppartcnstrmapFst)
	{
		ppartcnstrmapSnd->AddRef();
		return ppartcnstrmapSnd;
	}

	if (NULL == ppartcnstrmapSnd)
	{
		ppartcnstrmapFst->AddRef();
		return ppartcnstrmapFst;
	}

	GPOS_ASSERT(NULL != ppartcnstrmapFst);
	GPOS_ASSERT(NULL != ppartcnstrmapSnd);

	PartCnstrMap *ppartcnstrmap = GPOS_NEW(pmp) PartCnstrMap(pmp);

	CopyPartConstraints(pmp, ppartcnstrmap, ppartcnstrmapFst);
	CopyPartConstraints(pmp, ppartcnstrmap, ppartcnstrmapSnd);

	return ppartcnstrmap;
}

// EOF

