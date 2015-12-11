//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalProject.cpp
//
//	@doc:
//		Implementation of project operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CColRefTable.h"
#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/base/CConstraintInterval.h"

#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalProject.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/operators/CScalarProjectElement.h"

using namespace gpopt;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::CLogicalProject
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CLogicalProject::CLogicalProject
	(
	IMemoryPool *pmp
	)
	:
	CLogicalUnary(pmp)
{}

	
//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PcrsDeriveOutput
//
//	@doc:
//		Derive output columns
//
//---------------------------------------------------------------------------
CColRefSet *
CLogicalProject::PcrsDeriveOutput
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
{
	GPOS_ASSERT(2 == exprhdl.UlArity());
	
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	
	// the scalar child defines additional columns
	pcrs->Union(exprhdl.Pdprel(0)->PcrsOutput());
	pcrs->Union(exprhdl.Pdpscalar(1)->PcrsDefined());
	
	return pcrs;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PkcDeriveKeys
//
//	@doc:
//		Derive key collection
//
//---------------------------------------------------------------------------
CKeyCollection *
CLogicalProject::PkcDeriveKeys
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	return PkcDeriveKeysPassThru(exprhdl, 0 /* ulChild */);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PdrgpcrsEquivClassFromScIdent
//
//	@doc:
//		Return equivalence class from scalar ident project element
//
//---------------------------------------------------------------------------
DrgPcrs *
CLogicalProject::PdrgpcrsEquivClassFromScIdent
	(
	IMemoryPool *pmp,
	CExpression *pexprPrEl
	)
{
	GPOS_ASSERT(NULL != pexprPrEl);

	CScalarProjectElement *popPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());
	CColRef *pcrPrEl = popPrEl->Pcr();
	CExpression *pexprScalar = (*pexprPrEl)[0];


	if (EopScalarIdent != pexprScalar->Pop()->Eopid())
	{
		return NULL;
	}

	CScalarIdent *popScIdent = CScalarIdent::PopConvert(pexprScalar->Pop());
	const CColRef *pcrScIdent =  popScIdent->Pcr();
	GPOS_ASSERT(pcrPrEl->UlId() != pcrScIdent->UlId());
	GPOS_ASSERT(pcrPrEl->Pmdtype()->Pmdid()->FEquals(pcrScIdent->Pmdtype()->Pmdid()));

	if (!CUtils::FConstrainableType(pcrPrEl->Pmdtype()->Pmdid()))
	{
		return NULL;
	}

	// only add renamed columns to equivalent class if the column is not null-able
	// this is because equality predicates will be inferred from the equivalent class
	// during preprocessing
	if (CColRef::EcrtTable == pcrScIdent->Ecrt() &&
			!CColRefTable::PcrConvert(const_cast<CColRef*>(pcrScIdent))->FNullable())
	{
		// equivalence class
		DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

		CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrs->Include(pcrPrEl);
		pcrs->Include(pcrScIdent);
		pdrgpcrs->Append(pcrs);

		return pdrgpcrs;
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::ExtractConstraintFromScConst
//
//	@doc:
//		Extract constraint from scalar constant project element
//
//---------------------------------------------------------------------------
void
CLogicalProject::ExtractConstraintFromScConst
	(
	IMemoryPool *pmp,
	CExpression *pexprPrEl,
	DrgPcnstr *pdrgpcnstr, // array of range constraints
	DrgPcrs *pdrgpcrs // array of equivalence class
	)
{
	GPOS_ASSERT(NULL != pexprPrEl);
	GPOS_ASSERT(NULL != pdrgpcnstr);
	GPOS_ASSERT(NULL != pdrgpcrs);

	CScalarProjectElement *popPrEl = CScalarProjectElement::PopConvert(pexprPrEl->Pop());
	CColRef *pcr = popPrEl->Pcr();
	CExpression *pexprScalar = (*pexprPrEl)[0];

	IMDId *pmdidType = pcr->Pmdtype()->Pmdid();

	if (EopScalarConst != pexprScalar->Pop()->Eopid() ||
				!CUtils::FConstrainableType(pmdidType))
	{
		return;
	}

	CScalarConst *popConst = CScalarConst::PopConvert(pexprScalar->Pop());
	IDatum *pdatum = popConst->Pdatum();

	DrgPrng *pdrgprng = GPOS_NEW(pmp) DrgPrng(pmp);
	BOOL fNull = pdatum->FNull();
	if (!fNull)
	{
		pdatum->AddRef();
		pdrgprng->Append(GPOS_NEW(pmp) CRange
									(
									COptCtxt::PoctxtFromTLS()->Pcomp(),
									IMDType::EcmptEq,
									pdatum
									));
	}

	pdrgpcnstr->Append(GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, fNull));

	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pcr);
	pdrgpcrs->Append(pcrs);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PpcDeriveConstraint
//
//	@doc:
//		Derive constraint property
//
//---------------------------------------------------------------------------
CPropConstraint *
CLogicalProject::PpcDeriveConstraint
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.Pdpscalar(1)->FHasSubquery())
	{
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	CExpression *pexprPrL = exprhdl.PexprScalarChild(1);
	GPOS_ASSERT(NULL != pexprPrL);

	DrgPcnstr *pdrgpcnstr = GPOS_NEW(pmp) DrgPcnstr(pmp);
	DrgPcrs *pdrgpcrs = GPOS_NEW(pmp) DrgPcrs(pmp);

	const ULONG ulProjElems = pexprPrL->UlArity();
	for (ULONG ul = 0; ul < ulProjElems; ul++)
	{
		CExpression *pexprPrEl = (*pexprPrL)[ul];
		CExpression *pexprProjected = (*pexprPrEl)[0];

		if (EopScalarConst == pexprProjected->Pop()->Eopid())
		{
			ExtractConstraintFromScConst(pmp, pexprPrEl, pdrgpcnstr, pdrgpcrs);
		}
		else
		{
			DrgPcrs *pdrgpcrsChild = PdrgpcrsEquivClassFromScIdent(pmp, pexprPrEl);

			if (NULL != pdrgpcrsChild)
			{
				// merge with the equivalence classes we have so far
				DrgPcrs *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);

				// clean up
				pdrgpcrs->Release();
				pdrgpcrsChild->Release();

				pdrgpcrs = pdrgpcrsMerged;
			}
		}
	}

	if (0 == pdrgpcnstr->UlLength() && 0 == pdrgpcrs->UlLength())
	{
		// no constants or equivalence classes found, so just return the same constraint property of the child
		pdrgpcnstr->Release();
		pdrgpcrs->Release();
		return PpcDeriveConstraintPassThru(exprhdl, 0 /*ulChild*/);
	}

	CPropConstraint *ppcChild = exprhdl.Pdprel(0 /*ulChild*/)->Ppc();

	// equivalence classes coming from child
	DrgPcrs *pdrgpcrsChild = ppcChild->PdrgpcrsEquivClasses();
	if (NULL != pdrgpcrsChild)
	{
		// merge with the equivalence classes we have so far
		DrgPcrs *pdrgpcrsMerged = CUtils::PdrgpcrsMergeEquivClasses(pmp, pdrgpcrs, pdrgpcrsChild);

		// clean up
		pdrgpcrs->Release();
		pdrgpcrs = pdrgpcrsMerged;
	}

	// constraint coming from child
	CConstraint *pcnstr = ppcChild->Pcnstr();
	if (NULL != pcnstr)
	{
		pcnstr->AddRef();
		pdrgpcnstr->Append(pcnstr);
	}

	CConstraint *pcnstrNew = CConstraint::PcnstrConjunction(pmp, pdrgpcnstr);

	return GPOS_NEW(pmp) CPropConstraint(pmp, pdrgpcrs, pcnstrNew);
}


//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::Maxcard
//
//	@doc:
//		Derive max card
//
//---------------------------------------------------------------------------
CMaxCard
CLogicalProject::Maxcard
	(
	IMemoryPool *, // pmp
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.Pdpscalar(1)->FHasNonScalarFunction())
	{
		// unbounded by default
		return CMaxCard();
	}

	// pass on max card of first child
	return exprhdl.Pdprel(0)->Maxcard();
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PxfsCandidates
//
//	@doc:
//		Get candidate xforms
//
//---------------------------------------------------------------------------
CXformSet *
CLogicalProject::PxfsCandidates
	(
	IMemoryPool *pmp
	) 
	const
{
	CXformSet *pxfs = GPOS_NEW(pmp) CXformSet(pmp);

	(void) pxfs->FExchangeSet(CXform::ExfSimplifyProjectWithSubquery);
	(void) pxfs->FExchangeSet(CXform::ExfProject2Apply);
	(void) pxfs->FExchangeSet(CXform::ExfProject2ComputeScalar);
 	(void) pxfs->FExchangeSet(CXform::ExfCollapseProject);

	return pxfs;
}

//---------------------------------------------------------------------------
//	@function:
//		CLogicalProject::PstatsDerive
//
//	@doc:
//		Derive statistics
//
//---------------------------------------------------------------------------
IStatistics *
CLogicalProject::PstatsDerive
	(
	IMemoryPool *pmp,
	CExpressionHandle &exprhdl,
	DrgPstat * // pdrgpstatCtxt
	)
	const
{
	HMUlDatum *phmuldatum = GPOS_NEW(pmp) HMUlDatum(pmp);

	// extract scalar constant expression that can be used for 
	// statistics calculation
	CExpression *pexprPrList = exprhdl.PexprScalarChild(1 /*ulChildIndex*/);
	const ULONG ulArity = pexprPrList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprPrElem = (*pexprPrList)[ul];
		GPOS_ASSERT(1 == pexprPrElem->UlArity());
		CColRef *pcr = CScalarProjectElement::PopConvert(pexprPrElem->Pop())->Pcr();

		CExpression *pexprScalar = (*pexprPrElem)[0];
		COperator *pop = pexprScalar->Pop();
		if (COperator::EopScalarConst == pop->Eopid())
		{
			IDatum *pdatum = CScalarConst::PopConvert(pop)->Pdatum();
			if (pdatum->FStatsMappable())
			{
				pdatum->AddRef();
#ifdef GPOS_DEBUG
				BOOL fInserted =
#endif
						phmuldatum->FInsert(GPOS_NEW(pmp) ULONG(pcr->UlId()), pdatum);
				GPOS_ASSERT(fInserted);
			}
		}
	}

	IStatistics *pstats = PstatsDeriveProject(pmp, exprhdl, phmuldatum);

	// clean up
	phmuldatum->Release();

	return pstats;
}


// EOF

