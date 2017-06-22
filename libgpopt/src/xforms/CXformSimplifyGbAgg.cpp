//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSimplifyGbAgg.cpp
//
//	@doc:
//		Implementation of simplifying an aggregate expression by finding
//		the minimal grouping columns based on functional dependencies
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CKeyCollection.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/COperator.h"
#include "gpopt/xforms/CXformSimplifyGbAgg.h"

using namespace gpmd;
using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::CXformSimplifyGbAgg
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSimplifyGbAgg::CXformSimplifyGbAgg
	(
	IMemoryPool *pmp
	)
	:
	CXformExploration
		(
		 // pattern
		GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalGbAgg(pmp),
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // relational child
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // scalar project list
					)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle;
//		aggregate must have grouping columns
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSimplifyGbAgg::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(exprhdl.Pop());

	GPOS_ASSERT(COperator::EgbaggtypeGlobal == popAgg->Egbaggtype());

	if (0 == popAgg->Pdrgpcr()->UlLength() || NULL != popAgg->PdrgpcrMinimal())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::FDropGbAgg
//
//	@doc:
//		Return true if GbAgg operator can be dropped because grouping
//		columns include a key
//
//---------------------------------------------------------------------------
BOOL
CXformSimplifyGbAgg::FDropGbAgg
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CXformResult *pxfres
	)
{
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	if (0 < pexprProjectList->UlArity())
	{
		// GbAgg cannot be dropped if Agg functions are computed
		return false;
	}

	CKeyCollection *pkc = CDrvdPropRelational::Pdprel(pexprRelational->PdpDerive())->Pkc();
	if (NULL == pkc)
	{
		// relational child does not have key
		return false;
	}

	const ULONG ulKeys = pkc->UlKeys();
	BOOL fDrop = false;
	for (ULONG ul = 0; !fDrop && ul < ulKeys; ul++)
	{
		DrgPcr *pdrgpcrKey = pkc->PdrgpcrKey(pmp, ul);
		CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp, pdrgpcrKey);
		pdrgpcrKey->Release();

		CColRefSet *pcrsGrpCols = GPOS_NEW(pmp) CColRefSet(pmp);
		pcrsGrpCols->Include(popAgg->Pdrgpcr());
		BOOL fGrpColsHasKey = pcrsGrpCols->FSubset(pcrs);

		pcrs->Release();
		pcrsGrpCols->Release();
		if (fGrpColsHasKey)
		{
			// Gb operator can be dropped
			pexprRelational->AddRef();
			CExpression *pexprResult =
				CUtils::PexprLogicalSelect(pmp, pexprRelational, CPredicateUtils::PexprConjunction(pmp, NULL));
			pxfres->Add(pexprResult);
			fDrop = true;
		}
	}

	return fDrop;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformSimplifyGbAgg::Transform
//
//	@doc:
//		Actual transformation to simplify a aggregate expression
//
//---------------------------------------------------------------------------
void
CXformSimplifyGbAgg::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(NULL != pxfres);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	if (FDropGbAgg(pmp, pexpr,pxfres))
	{
		 // grouping columns could be dropped, GbAgg is transformed to a Select
		return;
	}

	// extract components
	CLogicalGbAgg *popAgg = CLogicalGbAgg::PopConvert(pexpr->Pop());
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprProjectList = (*pexpr)[1];

	DrgPcr *pdrgpcr = popAgg->Pdrgpcr();
	CColRefSet *pcrsGrpCols = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsGrpCols->Include(pdrgpcr);

	CColRefSet *pcrsCovered = GPOS_NEW(pmp) CColRefSet(pmp);	// set of grouping columns covered by FD's
	CColRefSet *pcrsMinimal = GPOS_NEW(pmp) CColRefSet(pmp); // a set of minimal grouping columns based on FD's
	DrgPfd *pdrgpfd = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->Pdrgpfd();

	// collect grouping columns FD's
	const ULONG ulSize = (pdrgpfd == NULL) ? 0 : pdrgpfd->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CFunctionalDependency *pfd = (*pdrgpfd)[ul];
		if (pfd->FIncluded(pcrsGrpCols))
		{
			pcrsCovered->Include(pfd->PcrsDetermined());
			pcrsCovered->Include(pfd->PcrsKey());
			pcrsMinimal->Include(pfd->PcrsKey());
		}
	}
	BOOL fCovered = pcrsCovered->FEqual(pcrsGrpCols);
	pcrsGrpCols->Release();
	pcrsCovered->Release();

	if (!fCovered)
	{
		// the union of RHS of collected FD's does not cover all grouping columns
		pcrsMinimal->Release();
		return;
	}

	// create a new Agg with minimal grouping columns
	pdrgpcr->AddRef();

	CLogicalGbAgg *popAggNew = GPOS_NEW(pmp) CLogicalGbAgg(pmp, pdrgpcr, pcrsMinimal->Pdrgpcr(pmp), popAgg->Egbaggtype());
	pcrsMinimal->Release();
	GPOS_ASSERT(!popAgg->FMatch(popAggNew) && "Simplified aggregate matches original aggregate");

	pexprRelational->AddRef();
	pexprProjectList->AddRef();
	CExpression *pexprResult = GPOS_NEW(pmp) CExpression(pmp, popAggNew, pexprRelational, pexprProjectList);
	pxfres->Add(pexprResult);
}


// EOF
