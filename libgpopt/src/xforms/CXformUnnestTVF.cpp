//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformUnnestTVF.cpp
//
//	@doc:
//		Implementation of TVF unnesting xform
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpopt/xforms/CXformUnnestTVF.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::CXformUnnestTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUnnestTVF::CXformUnnestTVF
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
				GPOS_NEW(pmp) CLogicalTVF(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternMultiTree(pmp)) // variable number of args, each is a deep tree
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformUnnestTVF::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	const ULONG ulArity = exprhdl.UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		if (exprhdl.Pdpscalar(ul)->FHasSubquery())
		{
			// xform is applicable if TVF argument is a subquery
			return CXform::ExfpHigh;
		}
	}

	return CXform::ExfpNone;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::PdrgpcrSubqueries
//
//	@doc:
//		Return array of subquery column references in CTE consumer output
//		after mapping to consumer output
//
//---------------------------------------------------------------------------
DrgPcr *
CXformUnnestTVF::PdrgpcrSubqueries
	(
	IMemoryPool *pmp,
	CExpression *pexprCTEProd,
	CExpression *pexprCTECons
	)
{
	CExpression *pexprProject = (*pexprCTEProd)[0];
	GPOS_ASSERT(COperator::EopLogicalProject == pexprProject->Pop()->Eopid());

	DrgPcr *pdrgpcrProdOutput = CDrvdPropRelational::Pdprel(pexprCTEProd->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);
	DrgPcr *pdrgpcrConsOutput = CDrvdPropRelational::Pdprel(pexprCTECons->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);
	GPOS_ASSERT(pdrgpcrProdOutput->UlLength() == pdrgpcrConsOutput->UlLength());

	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	const ULONG ulPrjElems = (*pexprProject)[1]->UlArity();
	for (ULONG ulOuter = 0; ulOuter < ulPrjElems; ulOuter++)
	{
		CExpression *pexprPrjElem = (*(*pexprProject)[1])[ulOuter];
		if (CDrvdPropScalar::Pdpscalar((*pexprPrjElem)[0]->PdpDerive())->FHasSubquery())
		{
			CColRef *pcrProducer = CScalarProjectElement::PopConvert(pexprPrjElem->Pop())->Pcr();
			CColRef *pcrConsumer =  CUtils::PcrMap(pcrProducer, pdrgpcrProdOutput, pdrgpcrConsOutput);
			GPOS_ASSERT(NULL != pcrConsumer);

			pdrgpcr->Append(pcrConsumer);
		}
	}

	pdrgpcrProdOutput->Release();
	pdrgpcrConsOutput->Release();

	return pdrgpcr;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::PexprProjectSubqueries
//
//	@doc:
//		Collect subquery arguments and return a Project expression with
//		collected subqueries in project list
//
//---------------------------------------------------------------------------
CExpression *
CXformUnnestTVF::PexprProjectSubqueries
	(
	IMemoryPool *pmp,
	CExpression *pexprTVF
	)
{
	GPOS_ASSERT(COperator::EopLogicalTVF == pexprTVF->Pop()->Eopid());

	// collect subquery arguments
	DrgPexpr *pdrgpexprSubqueries = GPOS_NEW(pmp) DrgPexpr(pmp);
	const ULONG ulArity = pexprTVF->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprScalarChild = (*pexprTVF)[ul];
		if (CDrvdPropScalar::Pdpscalar(pexprScalarChild->PdpDerive())->FHasSubquery())
		{
			pexprScalarChild->AddRef();
			pdrgpexprSubqueries->Append(pexprScalarChild);
		}
	}
	GPOS_ASSERT(0 < pdrgpexprSubqueries->UlLength());

	CExpression *pexprCTG = CUtils::PexprLogicalCTGDummy(pmp);
	CExpression *pexprProject = CUtils::PexprAddProjection(pmp, pexprCTG, pdrgpexprSubqueries);
	pdrgpexprSubqueries->Release();

	return pexprProject;
}


//---------------------------------------------------------------------------
//	@function:
//		CXformUnnestTVF::Transform
//
//	@doc:
//		Actual transformation
//		for queries of the form 'SELECT * FROM func(1, (select 5))'
//		we generate a correlated CTE expression to execute subquery args
//		in different subplans, the resulting expression looks like this
//
//		+--CLogicalCTEAnchor (0)
//		   +--CLogicalLeftOuterCorrelatedApply
//			  |--CLogicalTVF (func) Columns: ["a" (0), "b" (1)]
//			  |  |--CScalarConst (1)     <-- constant arg
//			  |  +--CScalarIdent "ColRef_0005" (11)    <-- subquery arg replaced by column
//			  |--CLogicalCTEConsumer (0), Columns: ["ColRef_0005" (11)]
//			  +--CScalarConst (1)
//
//		where CTE(0) is a Project expression on subquery args
//
//
//---------------------------------------------------------------------------
void
CXformUnnestTVF::Transform
	(
	CXformContext *pxfctxt,
	CXformResult *pxfres,
	CExpression *pexpr
	)
	const
{
	GPOS_ASSERT(NULL != pxfctxt);
	GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
	GPOS_ASSERT(FCheckPattern(pexpr));

	IMemoryPool *pmp = pxfctxt->Pmp();

	// create a project expression on subquery arguments
	CExpression *pexprProject = PexprProjectSubqueries(pmp, pexpr);

	// create a CTE producer on top of the project
	CCTEInfo *pcteinfo = COptCtxt::PoctxtFromTLS()->Pcteinfo();
	const ULONG ulCTEId = pcteinfo->UlNextId();

	// construct CTE producer output from subquery columns
	DrgPcr *pdrgpcrOutput = GPOS_NEW(pmp) DrgPcr(pmp);
	const ULONG ulPrjElems = (*pexprProject)[1]->UlArity();
	for (ULONG ulOuter = 0; ulOuter < ulPrjElems; ulOuter++)
	{
		CExpression *pexprPrjElem = (*(*pexprProject)[1])[ulOuter];
		if (CDrvdPropScalar::Pdpscalar((*pexprPrjElem)[0]->PdpDerive())->FHasSubquery())
		{
			CColRef *pcrSubq = CScalarProjectElement::PopConvert(pexprPrjElem->Pop())->Pcr();
			pdrgpcrOutput->Append(pcrSubq);
		}
	}

	CExpression *pexprCTEProd = CXformUtils::PexprAddCTEProducer(pmp, ulCTEId, pdrgpcrOutput, pexprProject);
	pdrgpcrOutput->Release();
	pexprProject->Release();

	// create CTE consumer
	DrgPcr *pdrgpcrProducerOutput = CDrvdPropRelational::Pdprel(pexprCTEProd->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);
	DrgPcr *pdrgpcrConsumerOutput = CUtils::PdrgpcrCopy(pmp, pdrgpcrProducerOutput);
	CLogicalCTEConsumer *popConsumer = GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcrConsumerOutput);
	CExpression *pexprCTECons = GPOS_NEW(pmp) CExpression(pmp, popConsumer);
	pcteinfo->IncrementConsumers(ulCTEId);
	pdrgpcrProducerOutput->Release();

	// find columns corresponding to subqueries in consumer's output
	DrgPcr *pdrgpcrSubqueries = PdrgpcrSubqueries(pmp, pexprCTEProd, pexprCTECons);

	// create new function arguments by replacing subqueries with columns in CTE consumer output
	DrgPexpr *pdrgpexprNewArgs = GPOS_NEW(pmp) DrgPexpr(pmp);
	ULONG ulIndex = 0;
	const ULONG ulArity = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CExpression *pexprScalarChild = (*pexpr)[ul];
		if (CDrvdPropScalar::Pdpscalar(pexprScalarChild->PdpDerive())->FHasSubquery())
		{
			CColRef *pcr =(*pdrgpcrSubqueries)[ulIndex];
			pdrgpexprNewArgs->Append(CUtils::PexprScalarIdent(pmp, pcr));
			ulIndex++;
		}
		else
		{
			(*pexpr)[ul]->AddRef();
			pdrgpexprNewArgs->Append((*pexpr)[ul]);
		}
	}

	// finally, create correlated apply expression
	CLogicalTVF *popTVF = CLogicalTVF::PopConvert(pexpr->Pop());
	popTVF->AddRef();
	CExpression *pexprCorrApply =
		CUtils::PexprLogicalApply<CLogicalLeftOuterCorrelatedApply>
				(
				pmp,
				GPOS_NEW(pmp) CExpression(pmp, popTVF, pdrgpexprNewArgs),
				pexprCTECons,
				pdrgpcrSubqueries,
				COperator::EopScalarSubquery,
				CPredicateUtils::PexprConjunction(pmp, NULL /*pdrgpexpr*/)	// scalar expression is const True
				);

	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalCTEAnchor(pmp, ulCTEId),
			pexprCorrApply
			);

	// add alternative to transformation result
	pxfres->Add(pexprAlt);
}


// EOF

