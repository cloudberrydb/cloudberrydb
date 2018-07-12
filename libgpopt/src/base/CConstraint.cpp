//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraint.cpp
//
//	@doc:
//		Implementation of constraints
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoRef.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/IColConstraintsMapper.h"
#include "gpopt/base/CColConstraintsArrayMapper.h"
#include "gpopt/base/CColConstraintsHashMapper.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CConstraintConjunction.h"
#include "gpopt/base/CConstraintDisjunction.h"
#include "gpopt/base/CConstraintNegation.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarArrayCmp.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/operators/CPredicateUtils.h"

using namespace gpopt;

// initialize constant true
BOOL CConstraint::m_fTrue(true);

// initialize constant false
BOOL CConstraint::m_fFalse(false);

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::CConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraint::CConstraint
	(
	IMemoryPool *pmp
	)
	:
	m_phmcontain(NULL),
	m_pmp(pmp),
	m_pcrsUsed(NULL),
	m_pexprScalar(NULL)
{
	m_phmcontain = GPOS_NEW(m_pmp) HMConstraintContainment(m_pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::~CConstraint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraint::~CConstraint()
{
	CRefCount::SafeRelease(m_pexprScalar);
	m_phmcontain->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarArrayCmp
//
//	@doc:
//		Create constraint from scalar array comparison expression
//		originally generated for "scalar op ANY/ALL (array)" construct
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrFromScalarArrayCmp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarArrayCmp(pexpr));

	CScalarArrayCmp *popScArrayCmp = CScalarArrayCmp::PopConvert(pexpr->Pop());
	CScalarArrayCmp::EArrCmpType earrccmpt = popScArrayCmp->Earrcmpt();

	if ((CScalarArrayCmp::EarrcmpAny == earrccmpt  || CScalarArrayCmp::EarrcmpAll == earrccmpt) &&
		CPredicateUtils::FCompareIdentToConstArray(pexpr))
	{
		// column
#ifdef GPOS_DEBUG
		CScalarIdent *popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		GPOS_ASSERT (pcr == (CColRef *) popScId->Pcr());
#endif // GPOS_DEBUG

		// get comparison type
		IMDType::ECmpType ecmpt = CUtils::Ecmpt(popScArrayCmp->PmdidOp());

		if (IMDType::EcmptOther == ecmpt )
		{
			// unsupported comparison operator for constraint derivation

			return NULL;
		}

		CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);

		const ULONG ulArity = CUtils::UlScalarArrayArity(pexprArray);

		// When array size exceeds the constraint derivation threshold,
		// don't expand it into a DNF and don't derive constraints
		COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
		ULONG ulArrayExpansionThreshold = poconf->Phint()->UlArrayExpansionThreshold();

		if (ulArity > ulArrayExpansionThreshold)
		{
			return NULL;
		}

		DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

		for (ULONG ul = 0; ul < ulArity; ul++)
		{
			CScalarConst *popScConst = CUtils::PScalarArrayConstChildAt(pexprArray,ul);
			CConstraintInterval *pci =  CConstraintInterval::PciIntervalFromColConstCmp(pmp, pcr, ecmpt, popScConst);
			pdrgpcnstr->Append(pci);
		}

		if (earrccmpt == CScalarArrayCmp::EarrcmpAny)
		{
			// predicate is of the form 'A IN (1,2,3)'
			// return a disjunction of ranges {[1,1], [2,2], [3,3]}
			return GPOS_NEW(pmp) CConstraintDisjunction(pmp, pdrgpcnstr);
		}

		// predicate is of the form 'A NOT IN (1,2,3)'
		// return a conjunctive negation on {[1,1], [2,2], [3,3]}
		return GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcnstr);
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarExpr
//
//	@doc:
//		Create constraint from scalar expression and pass back any discovered
//		equivalence classes
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrFromScalarExpr
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPcrs **ppdrgpcrs // output equivalence classes
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());
	GPOS_ASSERT(NULL != ppdrgpcrs);
	GPOS_ASSERT(NULL == *ppdrgpcrs);

	(void) pexpr->PdpDerive();
	CDrvdPropScalar *pdpScalar = CDrvdPropScalar::Pdpscalar(pexpr->Pdp(CDrvdProp::EptScalar));

	CColRefSet *pcrs = pdpScalar->PcrsUsed();
	ULONG ulCols = pcrs->CElements();

	if (0 == ulCols)
	{
		// TODO:  - May 29, 2012: in case of an expr with no columns (e.g. 1 < 2),
		// possibly evaluate the expression, and return a "TRUE" or "FALSE" constraint
		return NULL;
	}

	if (1 == ulCols)
	{
		CColRef *pcr = pcrs->PcrFirst();
		if (!CUtils::FConstrainableType(pcr->Pmdtype()->Pmdid()))
		{
			return NULL;
		}

		CConstraint *pcnstr = NULL;
		*ppdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

		// first, try creating a single interval constraint from the expression
		pcnstr = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexpr, pcr);
		if (NULL == pcnstr && CUtils::FScalarArrayCmp(pexpr))
		{
			// if the interval creation failed, try creating a disjunction or conjunction
			// of several interval constraints in the array case
			pcnstr = PcnstrFromScalarArrayCmp(pmp, pexpr, pcr);
		}

		if (NULL != pcnstr)
		{
			AddColumnToEquivClasses(pmp, pcr, ppdrgpcrs);
		}
		return pcnstr;
	}

	switch (pexpr->Pop()->Eopid())
	{
		case COperator::EopScalarBoolOp:
			return PcnstrFromScalarBoolOp(pmp, pexpr, ppdrgpcrs);

		case COperator::EopScalarCmp:
			return PcnstrFromScalarCmp(pmp, pexpr, ppdrgpcrs);

		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrConjunction
//
//	@doc:
//		Create conjunction from array of constraints
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrConjunction
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr
	)
{
	return PcnstrConjDisj(pmp, pdrgpcnstr, true /*fConj*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrDisjunction
//
//	@doc:
//		Create disjunction from array of constraints
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrDisjunction
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr
	)
{
	return PcnstrConjDisj(pmp, pdrgpcnstr, false /*fConj*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrConjDisj
//
//	@doc:
//		Create conjunction/disjunction from array of constraints
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrConjDisj
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr,
	BOOL fConj
	)
{
	GPOS_ASSERT(NULL != pdrgpcnstr);

	CConstraint *pcnstr = NULL;

	const ULONG ulLen = pdrgpcnstr->UlLength();

	switch (ulLen)
	{
		case 0:
		{
			pdrgpcnstr->Release();
			break;
		}

		case 1:
		{
			pcnstr = (*pdrgpcnstr)[0];
			pcnstr->AddRef();
			pdrgpcnstr->Release();
			break;
		}

		default:
		{
			if (fConj)
			{
				pcnstr = GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcnstr);
			}
			else
			{
				pcnstr = GPOS_NEW(pmp) CConstraintDisjunction(pmp, pdrgpcnstr);
			}
		}
	}

	return pcnstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::AddColumnToEquivClasses
//
//	@doc:
//		Add column as a new equivalence class, if it is not already in one of the
//		existing equivalence classes
//
//---------------------------------------------------------------------------
void
CConstraint::AddColumnToEquivClasses
	(
	IMemoryPool *pmp,
	const CColRef *pcr,
	DrgPcrs **ppdrgpcrs
	)
{
	const ULONG ulLen = (*ppdrgpcrs)->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRefSet *pcrs = (**ppdrgpcrs)[ul];
		if (pcrs->FMember(pcr))
		{
			return;
		}
	}

	CColRefSet *pcrsNew = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsNew->Include(pcr);

	(*ppdrgpcrs)->Append(pcrsNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarCmp
//
//	@doc:
//		Create constraint from scalar comparison
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrFromScalarCmp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPcrs **ppdrgpcrs // output equivalence classes
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarCmp(pexpr));
	GPOS_ASSERT(NULL != ppdrgpcrs);
	GPOS_ASSERT(NULL == *ppdrgpcrs);

	CExpression *pexprLeft = (*pexpr)[0];
	CExpression *pexprRight = (*pexpr)[1];

	// check if the scalar comparison is over scalar idents
	if (COperator::EopScalarIdent == pexprLeft->Pop()->Eopid()
		&& COperator::EopScalarIdent == pexprRight->Pop()->Eopid())
	{
		CScalarIdent *popScIdLeft = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		const CColRef *pcrLeft =  popScIdLeft->Pcr();

		CScalarIdent *popScIdRight = CScalarIdent::PopConvert((*pexpr)[1]->Pop());
		const CColRef *pcrRight =  popScIdRight->Pcr();

		if (!CUtils::FConstrainableType(pcrLeft->Pmdtype()->Pmdid()) ||
			!CUtils::FConstrainableType(pcrRight->Pmdtype()->Pmdid()))
		{
			return NULL;
		}

		*ppdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
		if (CPredicateUtils::FEquality(pexpr))
		{
			// col1 = col2
			CColRefSet *pcrsNew = GPOS_NEW(pmp) CColRefSet(pmp);
			pcrsNew->Include(pcrLeft);
			pcrsNew->Include(pcrRight);

			(*ppdrgpcrs)->Append(pcrsNew);
		}

		// create NOT NULL constraints to both columns
		DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
		pdrgpcnstr->Append(CConstraintInterval::PciUnbounded(pmp, pcrLeft, false /*fIncludesNull*/));
		pdrgpcnstr->Append(CConstraintInterval::PciUnbounded(pmp, pcrRight, false /*fIncludesNull*/));
		return CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);
	}

	// TODO: , May 28, 2012; add support for other cases besides (col cmp col)

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarBoolOp
//
//	@doc:
//		Create constraint from scalar boolean expression
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrFromScalarBoolOp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPcrs **ppdrgpcrs // output equivalence classes
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
	GPOS_ASSERT(NULL != ppdrgpcrs);
	GPOS_ASSERT(NULL == *ppdrgpcrs);

	const ULONG ulArity= pexpr->UlArity();

	// Large IN/NOT IN lists that can not be converted into
	// CScalarArrayCmp, are expanded into its disjunctive normal form,
	// represented by a large boolean expression tree.
	// For instance constructs of the form:
	// "(expression1, expression2) scalar op ANY/ALL ((const-x1,const-y1), ... (const-xn,const-yn))"
	// Deriving constraints from this is quite expensive; hence don't
	// bother when the arity of OR exceeds the threshold
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
	ULONG ulArrayExpansionThreshold = poconf->Phint()->UlArrayExpansionThreshold();

	if (CPredicateUtils::FOr(pexpr) && ulArity > ulArrayExpansionThreshold)
	{
		return NULL;
	}

	*ppdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);
	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);

	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		DrgPcrs *pdrgpcrsChild = NULL;
		CConstraint *pcnstrChild = PcnstrFromScalarExpr(pmp, (*pexpr)[ul], &pdrgpcrsChild);
		if (NULL == pcnstrChild || pcnstrChild->FUnbounded())
		{
			CRefCount::SafeRelease(pcnstrChild);
			CRefCount::SafeRelease(pdrgpcrsChild);
			if (CPredicateUtils::FOr(pexpr))
			{
				pdrgpcnstr->Release();
				return NULL;
			}
			continue;
		}
		GPOS_ASSERT(NULL != pdrgpcrsChild);

		pdrgpcnstr->Append(pcnstrChild);
		DrgPcrs *pdrgpcrsMerged = PdrgpcrsMergeFromBoolOp(pmp, pexpr, *ppdrgpcrs, pdrgpcrsChild);

		(*ppdrgpcrs)->Release();
		*ppdrgpcrs = pdrgpcrsMerged;
		pdrgpcrsChild->Release();
	}

	const ULONG ulLen = pdrgpcnstr->UlLength();
	if (0 == ulLen)
	{
		pdrgpcnstr->Release();
		return NULL;
	}

	if (1 == ulLen)
	{
		CConstraint *pcnstrChild = (*pdrgpcnstr)[0];
		pcnstrChild->AddRef();
		pdrgpcnstr->Release();

		if (CPredicateUtils::FNot(pexpr))
		{
			return GPOS_NEW(pmp) CConstraintNegation(pmp, pcnstrChild);
		}

		return pcnstrChild;
	}

	// we know we have more than one child
	if (CPredicateUtils::FAnd(pexpr))
	{
		return GPOS_NEW(pmp) CConstraintConjunction(pmp, pdrgpcnstr);
	}

	if (CPredicateUtils::FOr(pexpr))
	{
		return GPOS_NEW(pmp) CConstraintDisjunction(pmp, pdrgpcnstr);
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PdrgpcrsMergeFromBoolOp
//
//	@doc:
//		Merge equivalence classes coming from children of a bool op
//
//---------------------------------------------------------------------------
DrgPcrs *
CConstraint::PdrgpcrsMergeFromBoolOp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPcrs *pdrgpcrsFst,
	DrgPcrs *pdrgpcrsSnd
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
	GPOS_ASSERT(NULL != pdrgpcrsFst);
	GPOS_ASSERT(NULL != pdrgpcrsSnd);

	if (CPredicateUtils::FAnd(pexpr))
	{
		// merge with the equivalence classes we have so far
		return CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrsFst, pdrgpcrsSnd);
	}

	if (CPredicateUtils::FOr(pexpr))
	{
		// in case of an OR, an equivalence class must be coming from all
		// children to be part of the output
		return CUtils::PdrgpcrsIntersectEquivClasses(pmp, pdrgpcrsFst, pdrgpcrsSnd);
	}

	GPOS_ASSERT(CPredicateUtils::FNot(pexpr));
	return GPOS_NEW(pmp) DrgPcrs(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PdrgpcnstrOnColumn
//
//	@doc:
//		Return a subset of the given constraints which reference the
//		given column
//
//---------------------------------------------------------------------------
DrgPcnstr *
CConstraint::PdrgpcnstrOnColumn
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr,
	CColRef *pcr,
	BOOL fExclusive		// returned constraints must reference ONLY the given col
	)
{
	DrgPcnstr *pdrgpcnstrSubset = GPOS_NEW(pmp) DrgPcnstr(pmp);

	const ULONG ulLen = pdrgpcnstr->UlLength();

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstr = (*pdrgpcnstr)[ul];
		CColRefSet *pcrs = pcnstr->PcrsUsed();

		// if the fExclusive flag is true, then pcr must be the only column
		if (pcrs->FMember(pcr) && (!fExclusive || 1 == pcrs->CElements()))
		{
			pcnstr->AddRef();
			pdrgpcnstrSubset->Append(pcnstr);
		}
	}

	return pdrgpcnstrSubset;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PexprScalarConjDisj
//
//	@doc:
//		Construct a conjunction or disjunction scalar expression from an
//		array of constraints
//
//---------------------------------------------------------------------------
CExpression *
CConstraint::PexprScalarConjDisj
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr,
	BOOL fConj
	)
	const
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulLen = pdrgpcnstr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CExpression *pexpr = (*pdrgpcnstr)[ul]->PexprScalar(pmp);
		pexpr->AddRef();
		pdrgpexpr->Append(pexpr);
	}

	if (fConj)
	{
		return CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);
	}

	return CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PdrgpcnstrFlatten
//
//	@doc:
//		Flatten an array of constraints to be used as children for a conjunction
//		or disjunction. If any of these children is of the same type then use
//		its children directly instead of having multiple levels of the same type
//
//---------------------------------------------------------------------------
DrgPcnstr *
CConstraint::PdrgpcnstrFlatten
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr,
	EConstraintType ect
	)
	const
{
	DrgPcnstr *pdrgpcnstrNew = GPOS_NEW(pmp) DrgPcnstr(pmp);

	const ULONG ulLen = pdrgpcnstr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstrChild = (*pdrgpcnstr)[ul];
		EConstraintType ectChild = pcnstrChild->Ect();

		if (EctConjunction == ectChild && EctConjunction == ect)
		{
			CConstraintConjunction *pcconj = (CConstraintConjunction *)pcnstrChild;
			CUtils::AddRefAppend<CConstraint, CleanupRelease>(pdrgpcnstrNew, pcconj->Pdrgpcnstr());
		}
		else if (EctDisjunction == ectChild && EctDisjunction == ect)
		{
			CConstraintDisjunction *pcdisj = (CConstraintDisjunction *)pcnstrChild;
			CUtils::AddRefAppend<CConstraint, CleanupRelease>(pdrgpcnstrNew, pcdisj->Pdrgpcnstr());
		}
		else
		{
			pcnstrChild->AddRef();
			pdrgpcnstrNew->Append(pcnstrChild);
		}
	}

	pdrgpcnstr->Release();
	return PdrgpcnstrDeduplicate(pmp, pdrgpcnstrNew, ect);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PdrgpcnstrDeduplicate
//
//	@doc:
//		Simplify an array of constraints to be used as children for a conjunction
//		or disjunction. If there are two or more elements that reference only one
//		particular column, these constraints are combined into one
//
//---------------------------------------------------------------------------
DrgPcnstr *
CConstraint::PdrgpcnstrDeduplicate
	(
	IMemoryPool *pmp,
	DrgPcnstr *pdrgpcnstr,
	EConstraintType ect
	)
	const
{
	DrgPcnstr *pdrgpcnstrNew = GPOS_NEW(pmp) DrgPcnstr(pmp);

	CAutoRef<CColRefSet> pcrsDeduped(GPOS_NEW(pmp) CColRefSet(pmp));
	CAutoRef<IColConstraintsMapper> arccm;

	const ULONG ulLen = pdrgpcnstr->UlLength();

	pdrgpcnstr->AddRef();
	if (ulLen >= 5)
	{
		arccm = GPOS_NEW(pmp) CColConstraintsHashMapper(pmp, pdrgpcnstr);
	}
	else
	{
		arccm = GPOS_NEW(pmp) CColConstraintsArrayMapper(pmp, pdrgpcnstr);
	}

	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CConstraint *pcnstrChild = (*pdrgpcnstr)[ul];
		CColRefSet *pcrs = pcnstrChild->PcrsUsed();

		// we only simplify constraints that reference a single column, otherwise
		// we add constraint as is
		if (1 < pcrs->CElements())
		{
			pcnstrChild->AddRef();
			pdrgpcnstrNew->Append(pcnstrChild);
			continue;
		}

		CColRef *pcr = pcrs->PcrFirst();
		if (pcrsDeduped->FMember(pcr))
		{
			// current constraint has already been combined with a previous one
			continue;
		}

		DrgPcnstr *pdrgpcnstrCol = arccm->PdrgPcnstrLookup(pcr);

		if (1 == pdrgpcnstrCol->UlLength())
		{
			// if there is only one such constraint, then no simplification
			// for this column
			pdrgpcnstrCol->Release();
			pcnstrChild->AddRef();
			pdrgpcnstrNew->Append(pcnstrChild);
			continue;
		}

		CExpression *pexpr = NULL;

		if (EctConjunction == ect)
		{
			pexpr = PexprScalarConjDisj(pmp, pdrgpcnstrCol, true /*fConj*/);
		}
		else
		{
			GPOS_ASSERT(EctDisjunction == ect);
			pexpr = PexprScalarConjDisj(pmp, pdrgpcnstrCol, false /*fConj*/);
		}
		pdrgpcnstrCol->Release();
		GPOS_ASSERT(NULL != pexpr);

		CConstraint *pcnstrNew = CConstraintInterval::PciIntervalFromScalarExpr(pmp, pexpr, pcr);
		GPOS_ASSERT(NULL != pcnstrNew);
		pexpr->Release();
		pdrgpcnstrNew->Append(pcnstrNew);
		pcrsDeduped->Include(pcr);
	}

	pdrgpcnstr->Release();

	return pdrgpcnstrNew;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::Phmcolconstr
//
//	@doc:
//		Construct mapping between columns and arrays of constraints
//
//---------------------------------------------------------------------------
HMColConstr *
CConstraint::Phmcolconstr
	(
	IMemoryPool *pmp,
	CColRefSet *pcrs,
	DrgPcnstr *pdrgpcnstr
	)
	const
{
	GPOS_ASSERT(NULL != m_pcrsUsed);

	HMColConstr *phmcolconstr = GPOS_NEW(pmp) HMColConstr(pmp);

	CColRefSetIter crsi(*pcrs);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		DrgPcnstr *pdrgpcnstrCol = PdrgpcnstrOnColumn(pmp, pdrgpcnstr, pcr, false /*fExclusive*/);

#ifdef GPOS_DEBUG
		BOOL fres =
#endif //GPOS_DEBUG
		phmcolconstr->FInsert(pcr, pdrgpcnstrCol);
		GPOS_ASSERT(fres);
	}

	return phmcolconstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrConjDisjRemapForColumn
//
//	@doc:
//		Return a copy of the conjunction/disjunction constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraint::PcnstrConjDisjRemapForColumn
	(
	IMemoryPool *pmp,
	CColRef *pcr,
	DrgPcnstr *pdrgpcnstr,
	BOOL fConj
	)
	const
{
	GPOS_ASSERT(NULL != pcr);

	DrgPcnstr *pdrgpcnstrNew = GPOS_NEW(pmp) DrgPcnstr(pmp);

	const ULONG ulLen = pdrgpcnstr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		// clone child
		CConstraint *pcnstrChild = (*pdrgpcnstr)[ul]->PcnstrRemapForColumn(pmp, pcr);
		GPOS_ASSERT(NULL != pcnstrChild);

		pdrgpcnstrNew->Append(pcnstrChild);
	}

	if (fConj)
	{
		return PcnstrConjunction(pmp, pdrgpcnstrNew);
	}
	return PcnstrDisjunction(pmp, pdrgpcnstrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::FContains
//
//	@doc:
//		Does the current constraint contain the given one?
//
//---------------------------------------------------------------------------
BOOL
CConstraint::FContains
	(
	CConstraint *pcnstr
	)
{
	if (FUnbounded())
	{
		return true;
	}

	if (NULL == pcnstr || pcnstr->FUnbounded())
	{
		return false;
	}

	if (this == pcnstr)
	{
		// a constraint always contains itself
		return true;
	}

	// check if we have computed this containment query before
	BOOL *pfContains = m_phmcontain->PtLookup(pcnstr);
	if (NULL != pfContains)
	{
		return *pfContains;
	}

	BOOL fContains = true;

	// for each column used by the current constraint, we have to make sure that
	// the constraint on this column contains the corresponding given constraint
	CColRefSetIter crsi(*m_pcrsUsed);
	while (fContains && crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		CConstraint *pcnstrColThis = Pcnstr(m_pmp, pcr);
		GPOS_ASSERT (NULL != pcnstrColThis);
		CConstraint *pcnstrColOther = pcnstr->Pcnstr(m_pmp, pcr);

		// convert each of them to interval (if they are not already)
		CConstraintInterval *pciThis = CConstraintInterval::PciIntervalFromConstraint(m_pmp, pcnstrColThis, pcr);
		CConstraintInterval *pciOther = CConstraintInterval::PciIntervalFromConstraint(m_pmp, pcnstrColOther, pcr);

		fContains = pciThis->FContainsInterval(m_pmp, pciOther);
		pciThis->Release();
		pciOther->Release();
		pcnstrColThis->Release();
		CRefCount::SafeRelease(pcnstrColOther);
	}

	// insert containment query into the local map
#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
		m_phmcontain->FInsert(pcnstr, PfVal(fContains));
	GPOS_ASSERT(fSuccess);

	return fContains;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::FEquals
//
//	@doc:
//		Equality function
//
//---------------------------------------------------------------------------
BOOL
CConstraint::FEquals
	(
	CConstraint *pcnstr
	)
{
	if (NULL == pcnstr || pcnstr->FUnbounded())
	{
		return FUnbounded();
	}

	// check for pointer equality first
	if (this == pcnstr)
	{
		return true;
	}

	return (this->FContains(pcnstr) && pcnstr->FContains(this));
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PrintConjunctionDisjunction
//
//	@doc:
//		Common functionality for printing conjunctions and disjunctions
//
//---------------------------------------------------------------------------
IOstream &
CConstraint::PrintConjunctionDisjunction
	(
	IOstream &os,
	DrgPcnstr *pdrgpcnstr
	)
	const
{
	EConstraintType ect = Ect();
	GPOS_ASSERT(EctConjunction == ect || EctDisjunction == ect);

	os << "(";
	const ULONG ulArity = pdrgpcnstr->UlLength();
	(*pdrgpcnstr)[0]->OsPrint(os);

	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		if (EctConjunction == ect)
		{
			os << " AND ";
		}
		else
		{
			os << " OR ";
		}
		(*pdrgpcnstr)[ul]->OsPrint(os);
	}
	os << ")";

	return os;
}

#ifdef GPOS_DEBUG
void
CConstraint::DbgPrint() const
{
	CAutoTrace at(m_pmp);
	(void) this->OsPrint(at.Os());
}
#endif  // GPOS_DEBUG

// EOF
