//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintInterval.cpp
//
//	@doc:
//		Implementation of interval constraints
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CConstraintDisjunction.h"
#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/CScalarArray.h"
#include "gpopt/operators/CScalarIdent.h"
#include "naucrates/md/IMDScalarOp.h"
#include "gpopt/base/CDatumSortedSet.h"
#include "gpos/common/CAutoRef.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::CConstraintInterval
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CConstraintInterval::CConstraintInterval
	(
	IMemoryPool *pmp,
	const CColRef *pcr,
	DrgPrng *pdrgprng,
	BOOL fIncludesNull
	)
	:
	CConstraint(pmp),
	m_pcr(pcr),
	m_pdrgprng(pdrgprng),
	m_fIncludesNull(fIncludesNull)
{
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(NULL != pdrgprng);
	m_pcrsUsed = GPOS_NEW(pmp) CColRefSet(pmp);
	m_pcrsUsed->Include(pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::~CConstraintInterval
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CConstraintInterval::~CConstraintInterval()
{
	m_pdrgprng->Release();
	m_pcrsUsed->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FContradiction
//
//	@doc:
//		Check if this constraint is a contradiction. An interval is a contradiction if
//		it has no ranges and the null flag is not set
//
//---------------------------------------------------------------------------
BOOL
CConstraintInterval::FContradiction() const
{
	return (!m_fIncludesNull && 0 == m_pdrgprng->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FUnbounded
//
//	@doc:
//		Check if this interval is unbounded. An interval is unbounded if
//		it has a (-inf, inf) range and the null flag is set
//
//---------------------------------------------------------------------------
BOOL
CConstraintInterval::FUnbounded() const
{
	return (m_fIncludesNull && 1 == m_pdrgprng->UlLength() && (*m_pdrgprng)[0]->FUnbounded());
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PcnstrCopyWithRemappedColumns
//
//	@doc:
//		Return a copy of the constraint with remapped columns
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::PcnstrCopyWithRemappedColumns
	(
	IMemoryPool *pmp,
	HMUlCr *phmulcr,
	BOOL fMustExist
	)
{
	CColRef *pcr = CUtils::PcrRemap(m_pcr, phmulcr, fMustExist);
	return PcnstrRemapForColumn(pmp, pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarExpr
//
//	@doc:
//		Create interval from scalar expression
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarExpr
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{

	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(pexpr->Pop()->FScalar());

	// expression must use at most one column
#ifdef GPOS_DEBUG
	CDrvdPropScalar *pdpScalar = CDrvdPropScalar::Pdpscalar(pexpr->PdpDerive());
	GPOS_ASSERT(1 >= pdpScalar->PcrsUsed()->CElements());
#endif //GPOS_DEBUG
	CConstraintInterval *pci = NULL;
	switch (pexpr->Pop()->Eopid())
	{
		case COperator::EopScalarNullTest:
			pci = PciIntervalFromScalarNullTest(pmp, pexpr, pcr);
			break;
		case COperator::EopScalarBoolOp:
			pci =  PciIntervalFromScalarBoolOp(pmp, pexpr, pcr);
			break;
		case COperator::EopScalarCmp:
			pci =  PciIntervalFromScalarCmp(pmp, pexpr, pcr);
			break;
		case COperator::EopScalarConst:
			{
				if (CUtils::FScalarConstTrue(pexpr))
				{
					pci =  CConstraintInterval::PciUnbounded(pmp, pcr, true/*fIncludesNull*/);
				}
				else
				{
					pci =  GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, GPOS_NEW(pmp) DrgPrng(pmp), false /*fIncludesNull*/);
				}
			}
			break;
		case COperator::EopScalarArrayCmp:
			if (GPOS_FTRACE(EopttraceArrayConstraints))
			{
				pci = CConstraintInterval::PcnstrIntervalFromScalarArrayCmp(pmp, pexpr, pcr);
			}
			break;
		default:
			pci = NULL;
	}

	return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraint::PcnstrFromScalarArrayCmp
//
//	@doc:
//		Create constraint from scalar array comparison expression. Returns
//		NULL if a constraint interval cannot be created. Has side effect of
//		removing duplicates
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PcnstrIntervalFromScalarArrayCmp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	if (!CPredicateUtils::FCompareIdentToConstArray(pexpr))
	{
		return NULL;
	}
#ifdef GPOS_DEBUG
	else
	{
		// verify column in expr is the same as column which was passed in
		CScalarIdent *popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		GPOS_ASSERT(pcr == (CColRef *) popScId->Pcr());
	}
#endif // GPOS_DEBUG

	CScalarArrayCmp *popScArrayCmp = CScalarArrayCmp::PopConvert(pexpr->Pop());
	IMDType::ECmpType ecmpt = CUtils::Ecmpt(popScArrayCmp->PmdidOp());


	CExpression *pexprArray = CUtils::PexprScalarArrayChild(pexpr);
	const ULONG ulArrayExprArity = CUtils::UlScalarArrayArity(pexprArray);
	if (0 == ulArrayExprArity)
	{
		return NULL;
	}

	const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();
	gpos::CAutoRef<CDatumSortedSet> apdatumsortedset(GPOS_NEW(pmp) CDatumSortedSet(pmp, pexprArray, pcomp));
	// construct ranges representing IN or NOT IN
	DrgPrng *prgrng = GPOS_NEW(pmp) DrgPrng(pmp);

	switch(ecmpt)
	{
		case IMDType::EcmptEq:
		{
			// IN case, create ranges [X, X] [Y, Y] [Z, Z]
			for (ULONG ul = 0; ul < apdatumsortedset->UlLength(); ul++)
			{
				(*apdatumsortedset)[ul]->AddRef();
				CRange *prng = GPOS_NEW(pmp) CRange(pcomp, IMDType::EcmptEq, (*apdatumsortedset)[ul]);
				prgrng->Append(prng);
			}
			break;
		}
		case IMDType::EcmptNEq:
		{
			// NOT IN case, create ranges: (-inf, X) (X, Y) (Y, Z) (Z, inf)
			IDatum *pprevdatum = NULL;
			IDatum *pdatum = NULL;

			for (ULONG ul = 0; ul < apdatumsortedset->UlLength(); ul++)
			{
				if (0 != ul)
				{
					pprevdatum->AddRef();
				}

				pdatum = (*apdatumsortedset)[ul];
				pdatum->AddRef();

				IMDId *pmdid = pdatum->Pmdid();
				pmdid->AddRef();

				CRange *prng = GPOS_NEW(pmp) CRange(pmdid, pcomp, pprevdatum, CRange::EriExcluded, pdatum, CRange::EriExcluded);
				prgrng->Append(prng);

				pprevdatum = pdatum;
			}

			// add the last datum, making range (last, inf)
			IMDId *pmdid = pprevdatum->Pmdid();
			pprevdatum->AddRef();
			pmdid->AddRef();
			CRange *prng = GPOS_NEW(pmp) CRange(pmdid, pcomp, pprevdatum, CRange::EriExcluded, NULL, CRange::EriExcluded);
			prgrng->Append(prng);
			break;
		}
		default:
		{
			// does not handle IS DISTINCT FROM
			prgrng->Release();
			return NULL;
		}
	}

	BOOL fContainsNull = apdatumsortedset->FIncludesNull();

	return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, prgrng, fContainsNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromConstraint
//
//	@doc:
//		Create interval from any general constraint that references only one column
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromConstraint
	(
	IMemoryPool *pmp,
	CConstraint *pcnstr,
	CColRef *pcr
	)
{
	if (NULL == pcnstr)
	{
		GPOS_ASSERT(NULL != pcr && "Must provide valid column reference to construct unbounded interval");
		return PciUnbounded(pmp, pcr, true /*fIncludesNull*/);
	}

	if (CConstraint::EctInterval == pcnstr->Ect())
	{
		pcnstr->AddRef();
		return dynamic_cast<CConstraintInterval*>(pcnstr);
	}

	CColRefSet *pcrsUsed = pcnstr->PcrsUsed();
	GPOS_ASSERT (1 == pcrsUsed->CElements());

	CColRef *pcrFirst = pcrsUsed->PcrFirst();
	GPOS_ASSERT_IMP(NULL != pcr, pcrFirst == pcr);

	CExpression *pexprScalar = pcnstr->PexprScalar(pmp);

	return PciIntervalFromScalarExpr(pmp, pexprScalar, pcrFirst);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarNullTest
//
//	@doc:
//		Create interval from scalar null test
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarNullTest
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarNullTest(pexpr));

	// child of comparison operator
	CExpression *pexprChild = (*pexpr)[0];

	// TODO:  - May 28, 2012; add support for other expression forms
	// besides (ident is null)

	if (CUtils::FScalarIdent(pexprChild))
	{
#ifdef GPOS_DEBUG
		CScalarIdent *popScId = CScalarIdent::PopConvert(pexprChild->Pop());
		GPOS_ASSERT (pcr == (CColRef *) popScId->Pcr());
#endif // GPOS_DEBUG
		return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, GPOS_NEW(pmp) DrgPrng(pmp), true /*fIncludesNull*/);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarCmp
//
//	@doc:
//		Helper for create interval from comparison between a column and
//		a constant
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromColConstCmp
	(
	IMemoryPool *pmp,
	CColRef *pcr,
	IMDType::ECmpType ecmpt,
	CScalarConst *popScConst
	)
{
	CConstraintInterval *pcri = NULL;
	DrgPrng *pdrngprng = PciRangeFromColConstCmp(pmp, ecmpt, popScConst);
	if (NULL != pdrngprng)
	{
		pcri = GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrngprng, false /*fIncludesNull*/);
	}
	return pcri;

}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarCmp
//
//	@doc:
//		Create interval from scalar comparison expression
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarCmp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarCmp(pexpr) || CUtils::FScalarArrayCmp(pexpr));

	// TODO:  - May 28, 2012; add support for other expression forms
	// besides (column relop const)
	if (CPredicateUtils::FCompareIdentToConst(pexpr))
	{
		// column
#ifdef GPOS_DEBUG
		CScalarIdent *popScId = CScalarIdent::PopConvert((*pexpr)[0]->Pop());
		GPOS_ASSERT (pcr == (CColRef *) popScId->Pcr());
#endif // GPOS_DEBUG

		// constant
		CScalarConst *popScConst = CScalarConst::PopConvert((*pexpr)[1]->Pop());
		CScalarCmp *popScCmp = CScalarCmp::PopConvert(pexpr->Pop());

		return PciIntervalFromColConstCmp(pmp, pcr, popScCmp->Ecmpt(), popScConst);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolOp
//
//	@doc:
//		Create interval from scalar boolean: AND, OR, NOT
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarBoolOp
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));

	CScalarBoolOp *popScBool = CScalarBoolOp::PopConvert(pexpr->Pop());
	CScalarBoolOp::EBoolOperator eboolop = popScBool->Eboolop();

	switch (eboolop)
	{
		case CScalarBoolOp::EboolopAnd:
			return PciIntervalFromScalarBoolAnd(pmp, pexpr, pcr);

		case CScalarBoolOp::EboolopOr:
			return PciIntervalFromScalarBoolOr(pmp, pexpr, pcr);

		case CScalarBoolOp::EboolopNot:
		{
			CConstraintInterval *pciChild = PciIntervalFromScalarExpr(pmp, (*pexpr)[0], pcr);
			if (NULL == pciChild)
			{
				return NULL;
			}

			CConstraintInterval *pciNot = pciChild->PciComplement(pmp);
			pciChild->Release();
			return pciNot;
		}
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolOr
//
//	@doc:
//		Create interval from scalar boolean OR
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarBoolOr
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
	GPOS_ASSERT(CScalarBoolOp::EboolopOr == CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop());

	const ULONG ulArity = pexpr->UlArity();
	GPOS_ASSERT(0 < ulArity);

	CConstraintInterval *pci = PciIntervalFromScalarExpr(pmp, (*pexpr)[0], pcr);
	if (NULL == pci)
	{
		return NULL;
	}

	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		CConstraintInterval *pciChild = PciIntervalFromScalarExpr(pmp, (*pexpr)[ul], pcr);

		if (NULL == pciChild)
		{
			pci->Release();
			return NULL;
		}

		CConstraintInterval *pciOr = pci->PciUnion(pmp, pciChild);
		pci->Release();
		pciChild->Release();
		pci = pciOr;
	}

	return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntervalFromScalarBoolAnd
//
//	@doc:
//		Create interval from scalar boolean AND
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntervalFromScalarBoolAnd
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CUtils::FScalarBoolOp(pexpr));
	GPOS_ASSERT(CScalarBoolOp::EboolopAnd == CScalarBoolOp::PopConvert(pexpr->Pop())->Eboolop());

	const ULONG ulArity = pexpr->UlArity();
	GPOS_ASSERT(0 < ulArity);

	CConstraintInterval *pci = PciIntervalFromScalarExpr(pmp, (*pexpr)[0], pcr);
	for (ULONG ul = 1; ul < ulArity; ul++)
	{
		CConstraintInterval *pciChild = PciIntervalFromScalarExpr(pmp, (*pexpr)[ul], pcr);
		// here is where we will return a NULL child from not being able to create a
		// CConstraint interval from the ScalarExpr
		if (NULL != pciChild && NULL != pci)
		{
			CConstraintInterval *pciAnd = pci->PciIntersect(pmp, pciChild);
			pci->Release();
			pciChild->Release();
			pci = pciAnd;
		}
		else if (NULL != pciChild)
		{
			pci = pciChild;
		}
	}

	return pci;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprScalar
//
//	@doc:
//		Return scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprScalar
	(
	IMemoryPool *pmp
	)
{
	if (NULL == m_pexprScalar)
	{
		m_pexprScalar = PexprConstructScalar(pmp);
	}

	return m_pexprScalar;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructScalar
//
//	@doc:
//		Construct scalar expression
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructScalar
	(
	IMemoryPool *pmp
	)
	const
{
	if (FContradiction())
	{
		return CUtils::PexprScalarConstBool(pmp, false /*fval*/, false /*fNull*/);
	}

	if (GPOS_FTRACE(EopttraceArrayConstraints))
	{
		// try creating an array IN/NOT IN expression
		CExpression *pexpr = PexprConstructArrayScalar(pmp);
		if (pexpr != NULL)
		{
			return pexpr;
		}
	}

	// otherwise, we generate a disjunction of ranges
	return PexprConstructDisjunctionScalar(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructDisjunctionScalar
//
//	@doc:
//		Returns a disjunction of several equality or inequality expressions
//		describing this interval. Or, returns a singular expression if the
//		interval can be represented as such.
//		For example an interval containing ranges like
//			[1,1],(7,inf)
//		converts to an expression like
//			x = 1 OR x > 7
//		but an interval containing the range
//			(-inf, inf)
//		converts to a scalar true
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructDisjunctionScalar
	(
		IMemoryPool *pmp
	)
	const
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	const ULONG ulLen = m_pdrgprng->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CRange *prange = (*m_pdrgprng)[ul];
		CExpression *pexprChild = prange->PexprScalar(pmp, m_pcr);
		pdrgpexpr->Append(pexprChild);
	}

	if (1 == pdrgpexpr->UlLength() && CUtils::FScalarConstTrue((*pdrgpexpr)[0]))
	{
		// so far, interval covers all the not null values
		pdrgpexpr->Release();

		if (m_fIncludesNull)
		{
			return CUtils::PexprScalarConstBool(pmp, true /*fval*/, false /*fNull*/);
		}

		return CUtils::PexprIsNotNull(pmp, CUtils::PexprScalarIdent(pmp, m_pcr));
	}

	if (m_fIncludesNull)
	{
		CExpression *pexprIsNull = CUtils::PexprIsNull(pmp, CUtils::PexprScalarIdent(pmp, m_pcr));
		pdrgpexpr->Append(pexprIsNull);
	}

	return CPredicateUtils::PexprDisjunction(pmp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FConvertsToIn
//
//	@doc:
//		Looks for a specific pattern within the array of ranges to determine
//		if this interval can be converted into an array IN statement. The
//		pattern is like [[n,n], [m,m]] is an IN
//
//---------------------------------------------------------------------------
bool CConstraintInterval::FConvertsToIn() const
{
	if (1 >= m_pdrgprng->UlLength())
	{
		return false;
	}

	bool isIN = true;
	const ULONG ulLen = m_pdrgprng->UlLength();
	for (ULONG ul = 0; ul < ulLen && isIN; ul++)
	{
		isIN &= (*m_pdrgprng)[ul]->FPoint();
	}
	return isIN;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FConvertsToNotIn
//
//	@doc:
//		Looks for a specific pattern within the array of ranges to determine
//		if this interval can be converted into an array NOT IN statement. The
//		pattern is like [(-inf, m), (m, n), (n, inf)]
//
//---------------------------------------------------------------------------
bool CConstraintInterval::FConvertsToNotIn() const
{
	if (1 >= m_pdrgprng->UlLength())
	{
		return false;
	}

	// for this to be a NOT IN, its edges must be unbounded
	if ((*m_pdrgprng)[0]->PdatumLeft() != NULL ||
		(*m_pdrgprng)[m_pdrgprng->UlLength() - 1]->PdatumRight() != NULL)
	{
		return false;
	}

	// check that each range is exclusive and that the inner values are equal
	bool isNotIn = true;
	CRange *pLeftRng = (*m_pdrgprng)[0];
	CRange *pRightRng = NULL;
	const ULONG ulLen = m_pdrgprng->UlLength();
	for (ULONG ul = 1; ul < ulLen && isNotIn; ul++)
	{
		pRightRng = (*m_pdrgprng)[ul];
		isNotIn &= pLeftRng->EriRight() == CRange::EriExcluded;
		isNotIn &= pRightRng->EriLeft() == CRange::EriExcluded;
		isNotIn &= pLeftRng->FUpperBoundEqualsLowerBound(pRightRng);

		pLeftRng = pRightRng;
	}

	return isNotIn;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructArrayScalar
//
//	@doc:
//		Constructs an array expression from the ranges stored in this interval.
//		It is a mistake to call this method without first detecting if the
//		stored ranges can be converted to an IN or NOT in statement. The param
//		'fIn' refers to the statement being an IN statement, and if set to false,
// 		it is considered a NOT IN statement
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructArrayScalar(IMemoryPool *pmp, bool fIn) const
{
	GPOS_ASSERT(FConvertsToIn() || FConvertsToNotIn());

	ULONG ulRngs = m_pdrgprng->UlLength();
	IMDType::ECmpType ecmptype = IMDType::EcmptEq;
	CScalarArrayCmp::EArrCmpType earraycmptype = CScalarArrayCmp::EarrcmpAny;

	if (!fIn)
	{
		ecmptype = IMDType::EcmptNEq;
		earraycmptype = CScalarArrayCmp::EarrcmpAll;

		// if NOT IN, we skip the last range, as the right datum will be null
		ulRngs -= 1;
	}

	// loop through all of the constants in the ranges, creating an array of CScalarConst Expressions
	DrgPexpr *prngexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	// this method assumes IN or NOT IN which means that the ranges stored will look like either
	// [x,x], ... ,[y,y] or the NOT IN case (-inf, x),(x,y), ... ,(z,inf).
	for (ULONG ul = 0; ul < ulRngs; ul++)
	{
		IDatum *pdatum = (*m_pdrgprng)[ul]->PdatumRight();
		pdatum->AddRef();
		CScalarConst *popScConst = GPOS_NEW(pmp) CScalarConst(pmp, pdatum);
		CExpression *pexpr = GPOS_NEW(pmp) CExpression(pmp, popScConst);
		prngexpr->Append(pexpr);
	}

	if (m_fIncludesNull)
	{
		CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
		IDatum *pdatum = (*m_pdrgprng)[0]->PdatumRight();
		GPOS_ASSERT(NULL != pdatum);
		IDatum *pdatumNull = pmda->Pmdtype(pdatum->Pmdid())->PdatumNull();
		pdatumNull->AddRef();
		CScalarConst *popScConst = GPOS_NEW(pmp) CScalarConst(pmp, pdatumNull);
		prngexpr->Append(GPOS_NEW(pmp) CExpression(pmp, popScConst));
	}

	return CUtils::PexprScalarArrayCmp(pmp, earraycmptype, ecmptype, prngexpr, m_pcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PexprConstructScalar
//
//	@doc:
//		Constructs an array expression if the interval can be converted into
//		an array expression. Returns null if an array scalar cannot be
//		constructed
//
//---------------------------------------------------------------------------
CExpression *
CConstraintInterval::PexprConstructArrayScalar(IMemoryPool *pmp) const
{
	if (1 >= m_pdrgprng->UlLength())
	{
		return NULL;
	}

	if (FConvertsToIn())
	{
		return PexprConstructArrayScalar(pmp, true);
	}
	else if (FConvertsToNotIn())
	{
		return PexprConstructArrayScalar(pmp, false);
	}
	else
	{
		// Does not convert to either IN or NOT IN
		return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::Pcnstr
//
//	@doc:
//		Return constraint on a given column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::Pcnstr
	(
	IMemoryPool *, //pmp,
	const CColRef *pcr
	)
{
	if (m_pcr == pcr)
	{
		this->AddRef();
		return this;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::Pcnstr
//
//	@doc:
//		Return constraint on a given column set
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::Pcnstr
	(
	IMemoryPool *, //pmp,
	CColRefSet *pcrs
	)
{
	if (pcrs->FMember(m_pcr))
	{
		this->AddRef();
		return this;
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PcnstrRemapForColumn
//
//	@doc:
//		Return a copy of the constraint for a different column
//
//---------------------------------------------------------------------------
CConstraint *
CConstraintInterval::PcnstrRemapForColumn
	(
	IMemoryPool *pmp,
	CColRef *pcr
	)
	const
{
	GPOS_ASSERT(NULL != pcr);
	m_pdrgprng->AddRef();
	return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, m_pdrgprng, m_fIncludesNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciIntersect
//
//	@doc:
//		Intersection with another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciIntersect
	(
	IMemoryPool *pmp,
	CConstraintInterval *pci
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	DrgPrng *pdrgprngOther = pci->Pdrgprng();

	DrgPrng *pdrgprngNew = GPOS_NEW(pmp) DrgPrng(pmp);

	ULONG ulFst = 0;
	ULONG ulSnd = 0;
	const ULONG ulNumRangesFst = m_pdrgprng->UlLength();
	const ULONG ulNumRangesSnd = pdrgprngOther->UlLength();
	while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd)
	{
		CRange *prangeThis = (*m_pdrgprng)[ulFst];
		CRange *prangeOther = (*pdrgprngOther)[ulSnd];

		CRange *prangeNew = NULL;
		if (prangeOther->FEndsAfter(prangeThis))
		{
			prangeNew = prangeThis->PrngIntersect(pmp, prangeOther);
			ulFst ++;
		}
		else
		{
			prangeNew = prangeOther->PrngIntersect(pmp, prangeThis);
			ulSnd ++;
		}

		if (NULL != prangeNew)
		{
			pdrgprngNew->Append(prangeNew);
		}
	}

	return GPOS_NEW(pmp) CConstraintInterval
						(
						pmp,
						m_pcr,
						pdrgprngNew,
						m_fIncludesNull && pci->FIncludesNull()
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnion
//
//	@doc:
//		Union with another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciUnion
	(
	IMemoryPool *pmp,
	CConstraintInterval *pci
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	DrgPrng *pdrgprngOther = pci->Pdrgprng();

	DrgPrng *pdrgprngNew = GPOS_NEW(pmp) DrgPrng(pmp);

	ULONG ulFst = 0;
	ULONG ulSnd = 0;
	const ULONG ulNumRangesFst = m_pdrgprng->UlLength();
	const ULONG ulNumRangesSnd = pdrgprngOther->UlLength();
	while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd)
	{
		CRange *prangeThis = (*m_pdrgprng)[ulFst];
		CRange *prangeOther = (*pdrgprngOther)[ulSnd];

		CRange *prangeNew = NULL;
		if (prangeOther->FEndsAfter(prangeThis))
		{
			prangeNew = prangeThis->PrngDifferenceLeft(pmp, prangeOther);
			ulFst ++;
		}
		else
		{
			prangeNew = prangeOther->PrngDifferenceLeft(pmp, prangeThis);
			ulSnd ++;
		}

		AppendOrExtend(pmp, pdrgprngNew, prangeNew);
	}

	AddRemainingRanges(pmp, m_pdrgprng, ulFst, pdrgprngNew);
	AddRemainingRanges(pmp, pdrgprngOther, ulSnd, pdrgprngNew);

	return GPOS_NEW(pmp) CConstraintInterval
						(
						pmp,
						m_pcr,
						pdrgprngNew,
						m_fIncludesNull || pci->FIncludesNull()
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciDifference
//
//	@doc:
//		Difference between this interval and another interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciDifference
	(
	IMemoryPool *pmp,
	CConstraintInterval *pci
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	DrgPrng *pdrgprngOther = pci->Pdrgprng();

	DrgPrng *pdrgprngNew = GPOS_NEW(pmp) DrgPrng(pmp);

	ULONG ulFst = 0;
	ULONG ulSnd = 0;
	DrgPrng *pdrgprngResidual = GPOS_NEW(pmp) DrgPrng(pmp);
	CRange *prangeResidual = NULL;
	const ULONG ulNumRangesFst = m_pdrgprng->UlLength();
	const ULONG ulNumRangesSnd = pdrgprngOther->UlLength();
	while (ulFst < ulNumRangesFst && ulSnd < ulNumRangesSnd)
	{
		// if there is a residual range from previous iteration then use it
		CRange *prangeThis = (NULL == prangeResidual ? (*m_pdrgprng)[ulFst] : prangeResidual);
		CRange *prangeOther = (*pdrgprngOther)[ulSnd];

		CRange *prangeNew = NULL;
		prangeResidual = NULL;

		if (prangeOther->FEndsWithOrAfter(prangeThis))
		{
			prangeNew = prangeThis->PrngDifferenceLeft(pmp, prangeOther);
			ulFst ++;
		}
		else
		{
			prangeNew = PrangeDiffWithRightResidual(pmp, prangeThis, prangeOther, &prangeResidual, pdrgprngResidual);
			ulSnd ++;
		}

		AppendOrExtend(pmp, pdrgprngNew, prangeNew);
	}

	if (NULL != prangeResidual)
	{
		ulFst ++;
		prangeResidual->AddRef();
	}

	AppendOrExtend(pmp, pdrgprngNew, prangeResidual);
	pdrgprngResidual->Release();
	AddRemainingRanges(pmp, m_pdrgprng, ulFst, pdrgprngNew);

	return GPOS_NEW(pmp) CConstraintInterval
						(
						pmp,
						m_pcr,
						pdrgprngNew,
						m_fIncludesNull && !pci->FIncludesNull()
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::FContainsInterval
//
//	@doc:
//		Does the current interval contain the given interval?
//
//---------------------------------------------------------------------------
BOOL
CConstraintInterval::FContainsInterval
	(
	IMemoryPool *pmp,
	CConstraintInterval *pci
	)
{
	GPOS_ASSERT(NULL != pci);
	GPOS_ASSERT(m_pcr == pci->Pcr());

	if (FUnbounded())
	{
		return true;
	}

	if (NULL == pci ||
		pci->FUnbounded() ||
		(!FIncludesNull() && pci->FIncludesNull()))
	{
		return false;
	}

	CConstraintInterval *pciDiff = pci->PciDifference(pmp, this);

	// if the difference is empty, then this interval contains the given one
	BOOL fContains = pciDiff->FContradiction();
	pciDiff->Release();

	return fContains;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnbounded
//
//	@doc:
//		Create an unbounded interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciUnbounded
	(
	IMemoryPool *pmp,
	const CColRef *pcr,
	BOOL fIncludesNull
	)
{
	IMDId *pmdid = pcr->Pmdtype()->Pmdid();
	if (!CUtils::FConstrainableType(pmdid))
	{
		return NULL;
	}

	pmdid->AddRef();
	CRange *prange = GPOS_NEW(pmp) CRange
								(
								pmdid,
								COptCtxt::PoctxtFromTLS()->Pcomp(),
								NULL /*ppointLeft*/,
								CRange::EriExcluded,
								NULL /*ppointRight*/,
								CRange::EriExcluded
								);

	DrgPrng *pdrgprng = GPOS_NEW(pmp) DrgPrng(pmp);
	pdrgprng->Append(prange);

	return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, fIncludesNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciUnbounded
//
//	@doc:
//		Create an unbounded interval on any column from the given set
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciUnbounded
	(
	IMemoryPool *pmp,
	const CColRefSet *pcrs,
	BOOL fIncludesNull
	)
{
	// find the first constrainable column
	CColRefSetIter crsi(*pcrs);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		CConstraintInterval *pci = PciUnbounded(pmp, pcr, fIncludesNull);
		if (NULL != pci)
		{
			return pci;
		}
	}

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PmdidType
//
//	@doc:
//		Type of this interval
//
//---------------------------------------------------------------------------
IMDId *
CConstraintInterval::PmdidType()
{
	// if there is at least one range, return range type
	if (0 < m_pdrgprng->UlLength())
	{
		CRange *prange = (*m_pdrgprng)[0];
		return prange->Pmdid();
	}

	// otherwise return type of column ref
	return m_pcr->Pmdtype()->Pmdid();
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PciComplement
//
//	@doc:
//		Complement of this interval
//
//---------------------------------------------------------------------------
CConstraintInterval *
CConstraintInterval::PciComplement
	(
	IMemoryPool *pmp
	)
{
	// create an unbounded interval
	CConstraintInterval *pciUniversal = PciUnbounded(pmp, m_pcr, true /*fIncludesNull*/);

	CConstraintInterval *pciComp = pciUniversal->PciDifference(pmp, this);
	pciUniversal->Release();

	return pciComp;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::PrangeDiffWithRightResidual
//
//	@doc:
//		Difference between two ranges on the left side only -
//		Any difference on the right side is reported as residual range
//
//		this    |----------------------|
//		prange         |-----------|
//		result  |------|
//		residual                   |---|
//---------------------------------------------------------------------------
CRange *
CConstraintInterval::PrangeDiffWithRightResidual
	(
	IMemoryPool *pmp,
	CRange *prangeFirst,
	CRange *prangeSecond,
	CRange **pprangeResidual,
	DrgPrng *pdrgprngResidual
	)
{
	if (prangeSecond->FDisjointLeft(prangeFirst))
	{
		return NULL;
	}

	CRange *prangeRet = NULL;

	if (prangeFirst->FContains(prangeSecond))
	{
		prangeRet = prangeFirst->PrngDifferenceLeft(pmp, prangeSecond);
	}

	// the part of prangeFirst that goes beyond prangeSecond
	*pprangeResidual = prangeFirst->PrngDifferenceRight(pmp, prangeSecond);
	// add it to array so we can release it later on
	if (NULL != *pprangeResidual)
	{
		pdrgprngResidual->Append(*pprangeResidual);
	}

	return prangeRet;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::AddRemainingRanges
//
//	@doc:
//		Add ranges from a source array to a destination array, starting at the
//		range with the given index
//
//---------------------------------------------------------------------------
void
CConstraintInterval::AddRemainingRanges
	(
	IMemoryPool *pmp,
	DrgPrng *pdrgprngSrc,
	ULONG ulStart,
	DrgPrng *pdrgprngDest
	)
{
	const ULONG ulLen = pdrgprngSrc->UlLength();
	for (ULONG ul = ulStart; ul < ulLen; ul++)
	{
		CRange *prange = (*pdrgprngSrc)[ul];
		prange->AddRef();
		AppendOrExtend(pmp, pdrgprngDest, prange);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::AppendOrExtend
//
//	@doc:
//		Append the given range to the array or extend the last range in that
//		array
//
//---------------------------------------------------------------------------
void
CConstraintInterval::AppendOrExtend
	(
	IMemoryPool *pmp,
	DrgPrng *pdrgprng,
	CRange *prange
	)
{
	if (NULL == prange)
	{
		return;
	}

	GPOS_ASSERT(NULL != pdrgprng);

	const ULONG ulLen = pdrgprng->UlLength();
	if (0 == ulLen)
	{
		pdrgprng->Append(prange);
		return;
	}

	CRange *prangeLast = (*pdrgprng)[ulLen - 1];
	CRange *prangeNew = prangeLast->PrngExtend(pmp, prange);
	if (NULL == prangeNew)
	{
		pdrgprng->Append(prange);
	}
	else
	{
		pdrgprng->Replace(ulLen - 1, prangeNew);
		prange->Release();
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CConstraintInterval::OsPrint
//
//	@doc:
//		Debug print interval
//
//---------------------------------------------------------------------------
IOstream &
CConstraintInterval::OsPrint
	(
	IOstream &os
	)
	const
{
	os << "{";
	m_pcr->OsPrint(os);
	const ULONG ulLen = m_pdrgprng->UlLength();
	os << ", ranges: ";
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CRange *prange = (*m_pdrgprng)[ul];
		os << *prange << " ";
	}

	if (m_fIncludesNull)
	{
		os << "[NULL] ";
	}

	os << "}";

	return os;
}

//---------------------------------------------------------------------------
//	@function:
//		CRange::PciRangeFromColConstCmp
//
//	@doc:
//		Creates an array of 1 or 2 ranges which represent the comparison to
//		a scalar.
//
//---------------------------------------------------------------------------
DrgPrng*
CConstraintInterval::PciRangeFromColConstCmp
	(
	IMemoryPool *pmp,
	IMDType::ECmpType ecmpt,
	const CScalarConst *popsccnst
	)
{
	GPOS_ASSERT(CScalar::EopScalarConst == popsccnst->Eopid());

	// comparison operator
	if (IMDType::EcmptOther == ecmpt)
	{
		return NULL;
	}

	IDatum *pdatum = popsccnst->Pdatum();
	DrgPrng *pdrgprng = GPOS_NEW(pmp) DrgPrng(pmp);

	const IComparator *pcomp = COptCtxt::PoctxtFromTLS()->Pcomp();
	if (IMDType::EcmptNEq == ecmpt || IMDType::EcmptIDF == ecmpt)
	{
		// need an interval with 2 ranges
		pdatum->AddRef();
		pdrgprng->Append(GPOS_NEW(pmp) CRange(pcomp, IMDType::EcmptL, pdatum));
		pdatum->AddRef();
		pdrgprng->Append(GPOS_NEW(pmp) CRange(pcomp, IMDType::EcmptG, pdatum));
	}
	else
	{
		pdatum->AddRef();
		pdrgprng->Append(GPOS_NEW(pmp) CRange(pcomp, ecmpt, pdatum));
	}

	return pdrgprng;
}
// EOF
