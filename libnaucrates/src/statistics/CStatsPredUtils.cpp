//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright 2014 Pivotal Inc.
//
//	@filename:
//		CStatsPredUtils.cpp
//
//	@doc:
//		Statistics predicate helper routines
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/exception.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CExpressionUtils.h"
#include "gpopt/operators/CPredicateUtils.h"

#include "naucrates/statistics/CStatsPredUtils.h"
#include "naucrates/statistics/CStatisticsUtils.h"
#include "naucrates/statistics/CStatsPredLike.h"
#include "naucrates/statistics/CHistogram.h"

#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/md/IMDType.h"
#include "naucrates/statistics/CStatistics.h"
#include "naucrates/statistics/CStatsPredDisj.h"
#include "naucrates/statistics/CStatsPredConj.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Estatscmpt
//
//	@doc:
//		For the purpose of statistics computation, what is the comparison
//		type of an operator. Note that this has different, looser semantics
//		than CUtils::Ecmpt
//
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::Estatscmpt
	(
	const CWStringConst *pstrOpName
	)
{
	GPOS_ASSERT(NULL != pstrOpName);

	CStatsPred::EStatsCmpType escmpt = CStatsPred::EstatscmptOther;

	CWStringConst pstrL(GPOS_WSZ_LIT("<"));
	CWStringConst pstrLEq(GPOS_WSZ_LIT("<="));
	CWStringConst pstrEq(GPOS_WSZ_LIT("="));
	CWStringConst pstrGEq(GPOS_WSZ_LIT(">="));
	CWStringConst pstrG(GPOS_WSZ_LIT(">"));
	CWStringConst pstrNEq(GPOS_WSZ_LIT("<>"));

	if (pstrOpName->FEquals(&pstrL))
	{
		escmpt = CStatsPred::EstatscmptL;
	}
	if (pstrOpName->FEquals(&pstrLEq))
	{
		escmpt = CStatsPred::EstatscmptLEq;
	}
	if (pstrOpName->FEquals(&pstrEq))
	{
		escmpt = CStatsPred::EstatscmptEq;
	}
	if (pstrOpName->FEquals(&pstrGEq))
	{
		escmpt = CStatsPred::EstatscmptGEq;
	}
	if (pstrOpName->FEquals(&pstrG))
	{
		escmpt = CStatsPred::EstatscmptG;
	}
	if (pstrOpName->FEquals(&pstrNEq))
	{
		escmpt = CStatsPred::EstatscmptNEq;
	}

	return escmpt;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Estatscmpt
//
//	@doc:
//		For the purpose of statistics computation, what is the comparison
//		type of an operator. Note that this has different, looser semantics
//		than CUtils::Ecmpt
//
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::Estatscmpt
	(
	IMDId *pmdid
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDScalarOp *pmdscop = pmda->Pmdscop(pmdid);

	// Simply go by operator name.
	// If the name of the operator is "<", then it is a LessThan etc.
	const CWStringConst *pstrOpName = pmdscop->Mdname().Pstr();

	return Estatscmpt(pstrOpName);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredUnsupported
//
//	@doc:
//		Create an unsupported statistics predicate
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredUnsupported
	(
	IMemoryPool *pmp,
	CExpression *, // pexprPred,
	CColRefSet * //pcrsOuterRefs
	)
{
	return GPOS_NEW(pmp) CStatsPredUnsupported(ULONG_MAX, CStatsPred::EstatscmptOther);
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredNullTest
//
//	@doc:
//		Extract statistics filtering information from a null test
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredNullTest
	(
	IMemoryPool *pmp,
	CExpression *pexprPred,
	CColRefSet * //pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(FScalarIdentIsNull(pexprPred) || FScalarIdentIsNotNull(pexprPred));

	CExpression *pexprNullTest = pexprPred;
	CStatsPred::EStatsCmpType escmpt = CStatsPred::EstatscmptEq; // 'is null'

	if (FScalarIdentIsNotNull(pexprPred))
	{
		pexprNullTest = (*pexprPred)[0];
		escmpt = CStatsPred::EstatscmptNEq; // 'is not null'
	}

	CScalarIdent *popScalarIdent = CScalarIdent::PopConvert((*pexprNullTest)[0]->Pop());
	const CColRef *pcr = popScalarIdent->Pcr();

	IDatum *pdatum = CStatisticsUtils::PdatumNull(pcr);
	if (!pdatum->FStatsComparable(pdatum))
	{
		// stats calculations on such datums unsupported
		pdatum->Release();

		return GPOS_NEW(pmp) CStatsPredUnsupported(pcr->UlId(), escmpt);
	}

	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	CStatsPredPoint *pstatspred = GPOS_NEW(pmp) CStatsPredPoint(pcr->UlId(), escmpt, ppoint);

	return pstatspred;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredPoint
//
//	@doc:
//		Extract statistics filtering information from a point comparison
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredPoint
	(
	IMemoryPool *pmp,
	CExpression *pexprPred,
	CColRefSet *//pcrsOuterRefs,
	)
{
	GPOS_ASSERT(NULL != pexprPred);

	CStatsPred *pstatspred = Pstatspred(pmp, pexprPred);
	GPOS_ASSERT (NULL != pstatspred);

	return pstatspred;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Estatscmptype
//
//	@doc:
// 		Return the statistics predicate comparison type
//---------------------------------------------------------------------------
CStatsPred::EStatsCmpType
CStatsPredUtils::Estatscmptype
	(
	IMDId *pmdid
	)
{
	GPOS_ASSERT(NULL != pmdid);
	CStatsPred::EStatsCmpType escmpt = Estatscmpt(pmdid);

	if (CStatsPred::EstatscmptOther != escmpt)
	{
		return escmpt;
	}

	if (CPredicateUtils::FLikePredicate(pmdid))
	{
		return CStatsPred::EstatscmptLike;
	}

	return CStatsPred::EstatscmptOther;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Pstatspred
//
//	@doc:
// 		Generate a statistics point predicate for expressions that are supported.
//		Else an unsupported predicate. Note that the comparison semantics extracted are
//		for statistics computation only.
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::Pstatspred
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	CExpression *pexprLeft = NULL;
	CExpression *pexprRight = NULL;

	CStatsPred::EStatsCmpType escmpt = CStatsPred::EstatscmptOther;
	if (CPredicateUtils::FIdentIDFConstIgnoreCast(pexpr))
	{
		pexprLeft = (*pexpr)[0];
		pexprRight = (*pexpr)[1];

		escmpt = CStatsPred::EstatscmptIDF;
	}
	else if (CPredicateUtils::FIdentINDFConstIgnoreCast(pexpr))
	{
		CExpression *pexprChild = (*pexpr)[0];
		pexprLeft = (*pexprChild)[0];
		pexprRight = (*pexprChild)[1];

		escmpt = CStatsPred::EstatscmptINDF;
	}
	else
	{
		pexprLeft = (*pexpr)[0];
		pexprRight = (*pexpr)[1];

		GPOS_ASSERT(CPredicateUtils::FIdentCmpConstIgnoreCast(pexpr));

		COperator *pop = pexpr->Pop();
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pop);

		// Comparison semantics for statistics purposes is looser
		// than regular comparison
		escmpt = Estatscmptype(popScCmp->PmdidOp());
	}

	GPOS_ASSERT(COperator::EopScalarIdent == pexprLeft->Pop()->Eopid() || CScalarIdent::FCastedScId(pexprLeft));
	GPOS_ASSERT(COperator::EopScalarConst == pexprRight->Pop()->Eopid() || CScalarConst::FCastedConst(pexprRight));

	const CColRef *pcr = CUtils::PcrExtractFromScIdOrCastScId(pexprLeft);
	CScalarConst *popScalarConst = CScalarConst::PopExtractFromConstOrCastConst(pexprRight);
	GPOS_ASSERT(NULL != popScalarConst);

	IDatum *pdatum = popScalarConst->Pdatum();
	if (!CHistogram::FSupportsFilter(escmpt) || !IMDType::FStatsComparable(pcr->Pmdtype(), pdatum))
	{
		// case 1: unsupported predicate for stats calculations
		// example: SELECT 1 FROM pg_catalog.pg_class c WHERE c.relname ~ '^(t36)$';
		// case 2: unsupported stats comparison between the column and datum

		return GPOS_NEW(pmp) CStatsPredUnsupported(pcr->UlId(), escmpt);
	}

	return GPOS_NEW(pmp) CStatsPredPoint(pmp, pcr, escmpt, pdatum);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FCmpColsIgnoreCast
//
//	@doc:
// 		Is the expression a comparison of scalar ident or cast of a scalar ident?
//		Extract relevant info.
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FCmpColsIgnoreCast
	(
	CExpression *pexpr,
	const CColRef **ppcrLeft,
	CStatsPred::EStatsCmpType *pescmpt,
	const CColRef **ppcrRight
	)
{
	GPOS_ASSERT(NULL != ppcrLeft);
	GPOS_ASSERT(NULL != ppcrRight);
	COperator *pop = pexpr->Pop();

	BOOL fINDF = CPredicateUtils::FINDF(pexpr);
	BOOL fIDF = CPredicateUtils::FIDF(pexpr);
	BOOL fScalarCmp = (COperator::EopScalarCmp == pop->Eopid());
	if (!fScalarCmp && !fINDF && !fIDF)
	{
		return false;
	}

	CExpression *pexprLeft = NULL;
	CExpression *pexprRight = NULL;

	if (fINDF)
	{
		(*pescmpt) = CStatsPred::EstatscmptINDF;
		CExpression *pexprIDF = (*pexpr)[0];
		pexprLeft = (*pexprIDF)[0];
		pexprRight = (*pexprIDF)[1];
	}
	else if (fIDF)
	{
		(*pescmpt) = CStatsPred::EstatscmptIDF;
		pexprLeft = (*pexpr)[0];
		pexprRight = (*pexpr)[1];
	}
	else
	{
		GPOS_ASSERT(fScalarCmp);

		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pop);

		// Comparison semantics for stats purposes is looser
		// than regular comparison.
		(*pescmpt) = CStatsPredUtils::Estatscmpt(popScCmp->PmdidOp());

		pexprLeft = (*pexpr)[0];
		pexprRight = (*pexpr)[1];
	}

	(*ppcrLeft) = CUtils::PcrExtractFromScIdOrCastScId(pexprLeft);
	(*ppcrRight) = CUtils::PcrExtractFromScIdOrCastScId(pexprRight);

	if (NULL == *ppcrLeft || NULL == *ppcrRight)
	{
		// failed to extract a scalar ident
		return false;
	}

	return true;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredExtract
//
//	@doc:
//		Extract scalar expression for statistics filtering
//
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredExtract
	(
	IMemoryPool *pmp,
	CExpression *pexprScalar,
	CColRefSet *pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pexprScalar);
	if (CPredicateUtils::FOr(pexprScalar))
	{
		CStatsPred *pstatspredDisj = PstatspredDisj(pmp, pexprScalar, pcrsOuterRefs);
		if (NULL != pstatspredDisj)
		{
			return pstatspredDisj;
		}
	}
	else
	{
		CStatsPred *pstatspredConj = PstatspredConj(pmp, pexprScalar, pcrsOuterRefs);
		if (NULL != pstatspredConj)
		{
			return pstatspredConj;
		}
	}

	return GPOS_NEW(pmp) CStatsPredConj(GPOS_NEW(pmp) DrgPstatspred(pmp));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredConj
//
//	@doc:
//		Create conjunctive statistics filter composed of the extracted
//		components of the conjunction
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredConj
	(
	IMemoryPool *pmp,
	CExpression *pexprScalar,
	CColRefSet *pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pexprScalar);
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	const ULONG ulLen = pdrgpexprConjuncts->UlLength();

	DrgPstatspred *pdrgpstatspred = GPOS_NEW(pmp) DrgPstatspred(pmp);
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprConjuncts)[ul];
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();
		if (NULL != pcrsOuterRefs && pcrsOuterRefs->FSubset(pcrsUsed))
		{
			// skip predicate with outer references
			continue;
		}

		if (CPredicateUtils::FOr(pexprPred))
		{
			CStatsPred *pstatspredDisj = PstatspredDisj(pmp, pexprPred, pcrsOuterRefs);
			if (NULL != pstatspredDisj)
			{
				pdrgpstatspred->Append(pstatspredDisj);
			}
		}
		else
		{
			AddSupportedStatsFilters(pmp, pdrgpstatspred, pexprPred, pcrsOuterRefs);
		}
	}

	pdrgpexprConjuncts->Release();

	if (0 < pdrgpstatspred->UlLength())
	{
		return GPOS_NEW(pmp) CStatsPredConj(pdrgpstatspred);
	}

	pdrgpstatspred->Release();

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredDisj
//
//	@doc:
//		Create disjunctive statistics filter composed of the extracted
//		components of the disjunction
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredDisj
	(
	IMemoryPool *pmp,
	CExpression *pexprPred,
	CColRefSet *pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(CPredicateUtils::FOr(pexprPred));

	DrgPstatspred *pdrgpstatspredDisjChild = GPOS_NEW(pmp) DrgPstatspred(pmp);

	// remove duplicate components of the OR tree
	CExpression *pexprNew = CExpressionUtils::PexprDedupChildren(pmp, pexprPred);

	// extract the components of the OR tree
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprDisjuncts(pmp, pexprNew);
	const ULONG ulLen = pdrgpexpr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CExpression *pexpr = (*pdrgpexpr)[ul];
		CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive())->PcrsUsed();
		if (NULL != pcrsOuterRefs && pcrsOuterRefs->FSubset(pcrsUsed))
		{
			// skip predicate with outer references
			continue;
		}

		AddSupportedStatsFilters(pmp, pdrgpstatspredDisjChild, pexpr, pcrsOuterRefs);
	}

	// clean up
	pexprNew->Release();
	pdrgpexpr->Release();

	if (0 < pdrgpstatspredDisjChild->UlLength())
	{
		return GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredDisjChild);
	}

	pdrgpstatspredDisjChild->Release();

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::AddSupportedStatsFilters
//
//	@doc:
//		Add supported filter for statistics computation
//---------------------------------------------------------------------------
void
CStatsPredUtils::AddSupportedStatsFilters
	(
	IMemoryPool *pmp,
	DrgPstatspred *pdrgpstatspred,
	CExpression *pexprPred,
	CColRefSet *pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(NULL != pdrgpstatspred);

	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprPred->PdpDerive())->PcrsUsed();
	if (NULL != pcrsOuterRefs && pcrsOuterRefs->FSubset(pcrsUsed))
	{
		// skip predicates with outer references
		return;
	}

	if (COperator::EopScalarConst == pexprPred->Pop()->Eopid())
	{
		pdrgpstatspred->Append
							(
							GPOS_NEW(pmp) CStatsPredUnsupported
										(
										ULONG_MAX,
										CStatsPred::EstatscmptOther,
										CHistogram::DNeutralScaleFactor
										)
							);

		return;
	}

	if (COperator::EopScalarArrayCmp == pexprPred->Pop()->Eopid())
	{
		ProcessArrayCmp(pmp, pexprPred, pdrgpstatspred);
	}
	else
	{
		CStatsPredUtils::EPredicateType ept = Ept(pmp, pexprPred);
		GPOS_ASSERT(CStatsPredUtils::EptSentinel != ept);

		// array extract function mapping
		SScStatsfilterMapping rgStatsfilterTranslators[] =
		{
			{CStatsPredUtils::EptDisj, &CStatsPredUtils::PstatspredDisj},
			{CStatsPredUtils::EptScIdent, &CStatsPredUtils::PstatspredBoolean},
			{CStatsPredUtils::EptLike, &CStatsPredUtils::PstatspredLike},
			{CStatsPredUtils::EptPoint, &CStatsPredUtils::PstatspredPoint},
			{CStatsPredUtils::EptConj, &CStatsPredUtils::PstatspredConj},
			{CStatsPredUtils::EptNullTest, &CStatsPredUtils::PstatspredNullTest},
		};

		PfPstatspred *pf = &CStatsPredUtils::PstatspredUnsupported;
		const ULONG ulTranslators = GPOS_ARRAY_SIZE(rgStatsfilterTranslators);
		for (ULONG ul = 0; ul < ulTranslators; ul++)
		{
			SScStatsfilterMapping elem = rgStatsfilterTranslators[ul];
			if (ept == elem.ept)
			{
				pf = elem.pf;
				break;
			}
		}

		CStatsPred *pstatspred = pf(pmp, pexprPred, pcrsOuterRefs);
		if (NULL != pstatspred)
		{
			pdrgpstatspred->Append(pstatspred);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Ept
//
//	@doc:
//		Return statistics filter type of the given expression
//---------------------------------------------------------------------------
CStatsPredUtils::EPredicateType
CStatsPredUtils::Ept
	(
	IMemoryPool *pmp,
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	if (CPredicateUtils::FOr(pexprPred))
	{
		return CStatsPredUtils::EptDisj;
	}

	if (FBooleanScIdent(pexprPred))
	{
		return CStatsPredUtils::EptScIdent;
	}

	if (CPredicateUtils::FLikePredicate(pexprPred))
	{
		return CStatsPredUtils::EptLike;
	}

	if (FPointPredicate(pexprPred))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (FPointIDF(pexprPred))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (FPointINDF(pexprPred))
	{
		return CStatsPredUtils::EptPoint;
	}

	if (FConjunction(pmp, pexprPred))
	{
		return CStatsPredUtils::EptConj;
	}

	if (FScalarIdentIsNull(pexprPred) || FScalarIdentIsNotNull(pexprPred))
	{
		return CStatsPredUtils::EptNullTest;
	}

	return CStatsPredUtils::EptUnsupported;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FConjunction
//
//	@doc:
//		Is the condition a conjunctive predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FConjunction
	(
	IMemoryPool *pmp,
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprPred);
	const ULONG ulLen = pdrgpexprConjuncts->UlLength();
	pdrgpexprConjuncts->Release();

	return (1 < ulLen);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FBooleanScIdent
//
//	@doc:
//		Is the condition a boolean predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FBooleanScIdent
	(
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	return CPredicateUtils::FBooleanScalarIdent(pexprPred) || CPredicateUtils::FNegatedBooleanScalarIdent(pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FPointPredicate
//
//	@doc:
//		Is the condition a point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FPointPredicate
	(
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	return (CPredicateUtils::FIdentCmpConstIgnoreCast(pexprPred));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FPointIDF
//
//	@doc:
//		Is the condition an IDF point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FPointIDF
	(
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	return CPredicateUtils::FIdentIDFConstIgnoreCast(pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FPointINDF
//
//	@doc:
//		Is the condition an INDF point predicate
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FPointINDF
	(
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);

	if (!CPredicateUtils::FNot(pexprPred))
	{
		return false;
	}

	return FPointIDF((*pexprPred)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FScalarIdentIsNull
//
//	@doc:
//		Is the condition a 'is null' test on top a scalar ident
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FScalarIdentIsNull
	(
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);

	if (0 == pexprPred->UlArity())
	{
		return false;
	}
	// currently we support null test on scalar ident only
	return CUtils::FScalarNullTest(pexprPred) && CUtils::FScalarIdent((*pexprPred)[0]);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FScalarIdentNullTest
//
//	@doc:
//		Is the condition a not-null test
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FScalarIdentIsNotNull
	(
	CExpression *pexprPred
	)
{
	GPOS_ASSERT(NULL != pexprPred);

	if (0 == pexprPred->UlArity())
	{
		return false;
	}
	CExpression *pexprNullTest = (*pexprPred)[0];

	// currently we support not-null test on scalar ident only
	return CUtils::FScalarBoolOp(pexprPred, CScalarBoolOp::EboolopNot) && FScalarIdentIsNull(pexprNullTest);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredLikeHandleCasting
//
//	@doc:
//		Create a LIKE statistics filter
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredLike
	(
	IMemoryPool *pmp,
	CExpression *pexprPred,
	CColRefSet *//pcrsOuterRefs,
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(CPredicateUtils::FLikePredicate(pexprPred));

	CExpression *pexprLeft = (*pexprPred)[0];
	CExpression *pexprRight = (*pexprPred)[1];

	// we support LIKE predicate of the following patterns
	// CAST(ScIdent) LIKE Const
	// CAST(ScIdent) LIKE CAST(Const)
	// ScIdent LIKE Const
	// ScIdent LIKE CAST(Const)
	// CAST(Const) LIKE ScIdent
	// CAST(Const) LIKE CAST(ScIdent)
	// const LIKE ScIdent
	// const LIKE CAST(ScIdent)

	CExpression *pexprScIdent = NULL;
	CExpression *pexprScConst = NULL;

	CPredicateUtils::ExtractLikePredComponents(pexprPred, &pexprScIdent, &pexprScConst);

	if (NULL == pexprScIdent || NULL == pexprScConst)
	{
		return GPOS_NEW(pmp) CStatsPredUnsupported(ULONG_MAX, CStatsPred::EstatscmptLike);
	}

	CScalarIdent *popScalarIdent = CScalarIdent::PopConvert(pexprScIdent->Pop());
	ULONG ulColId = popScalarIdent->Pcr()->UlId();

	CScalarConst *popScalarConst = CScalarConst::PopConvert(pexprScConst->Pop());
	IDatum  *pdatumLiteral = popScalarConst->Pdatum();

	const CColRef *pcr = popScalarIdent->Pcr();
	if (!IMDType::FStatsComparable(pcr->Pmdtype(), pdatumLiteral))
	{
		// unsupported stats comparison between the column and datum
		return GPOS_NEW(pmp) CStatsPredUnsupported(pcr->UlId(), CStatsPred::EstatscmptLike);
	}

	CDouble dDefaultScaleFactor(1.0);
	if (pdatumLiteral->FSupportLikePredicate())
	{
		dDefaultScaleFactor = pdatumLiteral->DLikePredicateScaleFactor();
	}

	pexprLeft->AddRef();
	pexprRight->AddRef();

	return GPOS_NEW(pmp) CStatsPredLike(ulColId, pexprLeft, pexprRight, dDefaultScaleFactor);
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::ProcessArrayCmp
//
//	@doc:
//		Extract statistics filtering information from scalar array comparison
//---------------------------------------------------------------------------
void
CStatsPredUtils::ProcessArrayCmp
	(
	IMemoryPool *pmp,
	CExpression *pexprPred,
	DrgPstatspred *pdrgpstatspred
	)
{
	GPOS_ASSERT(NULL != pdrgpstatspred);
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(2 == pexprPred->UlArity());

	CScalarArrayCmp *popScArrayCmp = CScalarArrayCmp::PopConvert(pexprPred->Pop());
	CExpression *pexprIdent = (*pexprPred)[0];
	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexprPred);

	BOOL fCompareToConst = CPredicateUtils::FCompareIdentToConstArray(pexprPred);

	if (!fCompareToConst)
	{
		// unsupported predicate for stats calculations
		pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredUnsupported(ULONG_MAX, CStatsPred::EstatscmptOther));

		return;
	}

	BOOL fAny = (CScalarArrayCmp::EarrcmpAny == popScArrayCmp->Earrcmpt());

	DrgPstatspred *pdrgpstatspredChild = pdrgpstatspred;
	if (fAny)
	{
		pdrgpstatspredChild = GPOS_NEW(pmp) DrgPstatspred(pmp);
	}

	const ULONG ulConstants = CUtils::UlScalarArrayArity(pexprArray);
	// comparison semantics for statistics purposes is looser than regular comparison.
	CStatsPred::EStatsCmpType escmpt = Estatscmptype(popScArrayCmp->PmdidOp());

	CScalarIdent *popScalarIdent = CScalarIdent::PopConvert(pexprIdent->Pop());
	const CColRef *pcr = popScalarIdent->Pcr();

	if (!CHistogram::FSupportsFilter(escmpt))
	{
		// unsupported predicate for stats calculations
		pdrgpstatspred->Append(GPOS_NEW(pmp) CStatsPredUnsupported(pcr->UlId(), escmpt));

		return;
	}

	for (ULONG ul = 0; ul < ulConstants; ul++)
	{
		CExpression *pexprConst = CUtils::PScalarArrayExprChildAt(pmp, pexprArray, ul);
		if (COperator::EopScalarConst == pexprConst->Pop()->Eopid())
		{
			CScalarConst *popScalarConst = CScalarConst::PopConvert(pexprConst->Pop());
			IDatum *pdatumLiteral = popScalarConst->Pdatum();
			CStatsPred *pstatspredChild = NULL;
			if (!pdatumLiteral->FStatsComparable(pdatumLiteral))
			{
				// stats calculations on such datums unsupported
				pstatspredChild = GPOS_NEW(pmp) CStatsPredUnsupported(pcr->UlId(), escmpt);
			}
			else
			{
				pstatspredChild = GPOS_NEW(pmp) CStatsPredPoint(pmp, pcr, escmpt, pdatumLiteral);
			}

			pdrgpstatspredChild->Append(pstatspredChild);
		}
		pexprConst->Release();
	}

	if (fAny)
	{
		CStatsPredDisj *pstatspredOr = GPOS_NEW(pmp) CStatsPredDisj(pdrgpstatspredChild);
		pdrgpstatspred->Append(pstatspredOr);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatspredBoolean
//
//	@doc:
//		Extract statistics filtering information from boolean predicate
//		in the form of scalar id or negated scalar id
//---------------------------------------------------------------------------
CStatsPred *
CStatsPredUtils::PstatspredBoolean
	(
	IMemoryPool *pmp,
	CExpression *pexprPred,
	CColRefSet * //pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pexprPred);
	GPOS_ASSERT(CPredicateUtils::FBooleanScalarIdent(pexprPred) || CPredicateUtils::FNegatedBooleanScalarIdent(pexprPred));

	COperator *pop = pexprPred->Pop();

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	IDatum *pdatum = NULL;
	ULONG ulColId = ULONG_MAX;

	if (CPredicateUtils::FBooleanScalarIdent(pexprPred))
	{
		CScalarIdent *popScIdent = CScalarIdent::PopConvert(pop);
		pdatum = pmda->PtMDType<IMDTypeBool>()->PdatumBool(pmp, true /* fValue */, false /* fNull */);
		ulColId = popScIdent->Pcr()->UlId();
	}
	else
	{
		CExpression *pexprChild = (*pexprPred)[0];
		pdatum = pmda->PtMDType<IMDTypeBool>()->PdatumBool(pmp, false /* fValue */, false /* fNull */);
		ulColId = CScalarIdent::PopConvert(pexprChild->Pop())->Pcr()->UlId();
	}

	if (!pdatum->FStatsComparable(pdatum))
	{
		// stats calculations on such datums unsupported
		pdatum->Release();

		return GPOS_NEW(pmp) CStatsPredUnsupported(ulColId, CStatsPred::EstatscmptEq);
	}


	GPOS_ASSERT(NULL != pdatum && ULONG_MAX != ulColId);

	return GPOS_NEW(pmp) CStatsPredPoint(ulColId, CStatsPred::EstatscmptEq, GPOS_NEW(pmp) CPoint(pdatum));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PstatsjoinExtract
//
//	@doc:
//		Helper function to extract the statistics join filter from a given join predicate
//
//---------------------------------------------------------------------------
CStatisticsJoin *
CStatsPredUtils::PstatsjoinExtract
	(
	IMemoryPool *pmp,
	CExpression *pexprJoinPred,
	DrgPcrs *pdrgpcrsOutput, // array of output columns of join's relational inputs
	CColRefSet *pcrsOuterRefs,
	DrgPexpr *pdrgpexprUnsupported
	)
{
	GPOS_ASSERT(NULL != pexprJoinPred);
	GPOS_ASSERT(NULL != pdrgpcrsOutput);
	GPOS_ASSERT(NULL != pdrgpexprUnsupported);

	CColRefSet *pcrsUsed = CDrvdPropScalar::Pdpscalar(pexprJoinPred->PdpDerive())->PcrsUsed();

	if (pcrsOuterRefs->FSubset(pcrsUsed))
	{
		return NULL;
	}

	const CColRef *pcrLeft = NULL;
	const CColRef *pcrRight = NULL;
	CStatsPred::EStatsCmpType escmpt = CStatsPred::EstatscmptOther;

	BOOL fSupportedScIdentComparison = FCmpColsIgnoreCast(pexprJoinPred, &pcrLeft, &escmpt, &pcrRight);
	if (fSupportedScIdentComparison && CStatsPred::EstatscmptOther != escmpt)
	{
		if (!IMDType::FStatsComparable(pcrLeft->Pmdtype(), pcrRight->Pmdtype()))
		{
			// unsupported statistics comparison between the histogram boundaries of the columns
			pexprJoinPred->AddRef();
			pdrgpexprUnsupported->Append(pexprJoinPred);
			return NULL;
		}

		ULONG ulIndexLeft = CUtils::UlPcrIndexContainingSet(pdrgpcrsOutput, pcrLeft);
		ULONG ulIndexRight = CUtils::UlPcrIndexContainingSet(pdrgpcrsOutput, pcrRight);

		if (ULONG_MAX != ulIndexLeft && ULONG_MAX != ulIndexRight && ulIndexLeft != ulIndexRight)
		{
			if (ulIndexLeft < ulIndexRight)
			{
				return GPOS_NEW(pmp) CStatisticsJoin(pcrLeft->UlId(), escmpt, pcrRight->UlId());
			}

			return GPOS_NEW(pmp) CStatisticsJoin(pcrRight->UlId(), escmpt, pcrLeft->UlId());
		}
	}

	if (CColRefSet::FCovered(pdrgpcrsOutput, pcrsUsed))
	{
		// unsupported join predicate
		pexprJoinPred->AddRef();
		pdrgpexprUnsupported->Append(pexprJoinPred);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::PdrgpstatsjoinExtract
//
//	@doc:
//		Helper function to extract array of statistics join filter
//		from an array of join predicates
//
//---------------------------------------------------------------------------
DrgPstatsjoin *
CStatsPredUtils::PdrgpstatsjoinExtract
	(
	IMemoryPool *pmp,
	CExpression *pexprScalar,
	DrgPcrs *pdrgpcrsOutput, // array of output columns of join's relational inputs
	CColRefSet *pcrsOuterRefs,
	CStatsPred **ppstatspredUnsupported
	)
{
	GPOS_ASSERT(NULL != pexprScalar);
	GPOS_ASSERT(NULL != pdrgpcrsOutput);

	DrgPstatsjoin *pdrgpstatsjoin = GPOS_NEW(pmp) DrgPstatsjoin(pmp);

	DrgPexpr *pdrgpexprUnsupported = GPOS_NEW(pmp) DrgPexpr(pmp);

	// extract all the conjuncts
	DrgPexpr *pdrgpexprConjuncts = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	const ULONG ulSize = pdrgpexprConjuncts->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CExpression *pexprPred = (*pdrgpexprConjuncts) [ul];
		CStatisticsJoin *pstatsjoin = PstatsjoinExtract
										(
										pmp,
										pexprPred,
										pdrgpcrsOutput,
										pcrsOuterRefs,
										pdrgpexprUnsupported
										);
		if (NULL != pstatsjoin)
		{
			pdrgpstatsjoin->Append(pstatsjoin);
		}
	}

	const ULONG ulUnsupported = pdrgpexprUnsupported->UlLength();
	if (1 == ulUnsupported)
	{
		*ppstatspredUnsupported = CStatsPredUtils::PstatspredExtract(pmp, (*pdrgpexprUnsupported)[0], pcrsOuterRefs);
	}
	else if (1 < ulUnsupported)
	{
		pdrgpexprUnsupported->AddRef();
		CExpression *pexprConj = CPredicateUtils::PexprConjDisj(pmp, pdrgpexprUnsupported, true /* fConjunction */);
		*ppstatspredUnsupported = CStatsPredUtils::PstatspredExtract(pmp, pexprConj, pcrsOuterRefs);
		pexprConj->Release();
	}

	// clean up
	pdrgpexprUnsupported->Release();
	pdrgpexprConjuncts->Release();

	return pdrgpstatsjoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Pdrgpstatsjoin
//
//	@doc:
//		Helper function to extract array of statistics join filter from
//		an expression
//---------------------------------------------------------------------------
DrgPstatsjoin *
CStatsPredUtils::Pdrgpstatsjoin
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	CExpression *pexprScalarInput,
	DrgPcrs *pdrgpcrsOutput, // array of output columns of join's relational inputs
	CColRefSet *pcrsOuterRefs
	)
{
	GPOS_ASSERT(NULL != pdrgpcrsOutput);

	// remove implied predicates from join condition to avoid cardinality under-estimation
	CExpression *pexprScalar = CPredicateUtils::PexprRemoveImpliedConjuncts(pmp, pexprScalarInput, exprhdl);

	// extract all the conjuncts
	CStatsPred *pstatspredUnsupported = NULL;
	DrgPstatsjoin *pdrgpstatsjoin = PdrgpstatsjoinExtract
										(
										pmp,
										pexprScalar,
										pdrgpcrsOutput,
										pcrsOuterRefs,
										&pstatspredUnsupported
										);

	// TODO:  May 15 2014, handle unsupported predicates for LASJ, LOJ and LS joins
	// clean up
	CRefCount::SafeRelease(pstatspredUnsupported);
	pexprScalar->Release();

	return pdrgpstatsjoin;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::Pdrgpstatsjoin
//
//	@doc:
//		Extract statistics join information
//
//---------------------------------------------------------------------------
DrgPstatsjoin *
CStatsPredUtils::Pdrgpstatsjoin
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	// in case of subquery in join predicate, we return empty stats
	if (exprhdl.Pdpscalar(exprhdl.UlArity() - 1)->FHasSubquery())
	{
		return GPOS_NEW(pmp) DrgPstatsjoin(pmp);
	}

	DrgPcrs *pdrgpcrsOutput = GPOS_NEW(pmp) DrgPcrs(pmp);
	const ULONG ulSize = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulSize - 1; ul++)
	{
		CColRefSet *pcrs = exprhdl.Pdprel(ul)->PcrsOutput();
		pcrs->AddRef();
		pdrgpcrsOutput->Append(pcrs);
	}

	// TODO:  02/29/2012 replace with constraint property info once available
	CExpression *pexprScalar = exprhdl.PexprScalarChild(exprhdl.UlArity() - 1);
	CColRefSet *pcrsOuterRefs = exprhdl.Pdprel()->PcrsOuter();

	DrgPstatsjoin *pdrgpstats = Pdrgpstatsjoin(pmp, exprhdl, pexprScalar, pdrgpcrsOutput, pcrsOuterRefs);

	// clean up
	pdrgpcrsOutput->Release();

	return pdrgpstats;
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FConjOrDisjPred
//
//	@doc:
//		Is the predicate a conjunctive or disjunctive predicate
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FConjOrDisjPred
	(
	CStatsPred *pstatspred
	)
{
	return ((CStatsPred::EsptConj == pstatspred->Espt()) || (CStatsPred::EsptDisj == pstatspred->Espt()));
}


//---------------------------------------------------------------------------
//	@function:
//		CStatsPredUtils::FUnsupportedPredOnDefinedCol
//
//	@doc:
//		Is unsupported predicate on defined column
//
//---------------------------------------------------------------------------
BOOL
CStatsPredUtils::FUnsupportedPredOnDefinedCol
	(
	CStatsPred *pstatspred
	)
{
	return ((CStatsPred::EsptUnsupported == pstatspred->Espt()) && (ULONG_MAX == pstatspred->UlColId()));
}


// EOF
