//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CXformSelect2PartialDynamicIndexGet.cpp
//
//	@doc:
//		Implementation of select over a partitioned table to a dynamic index get
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/metadata/CPartConstraint.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformSelect2PartialDynamicIndexGet.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/IMDPartConstraint.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::CXformSelect2PartialDynamicIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2PartialDynamicIndexGet::CXformSelect2PartialDynamicIndexGet
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformExploration
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalSelect(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalDynamicGet(pmp)), // relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// predicate tree
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2PartialDynamicIndexGet::Exfp
	(
	CExpressionHandle &exprhdl
	)
	const
{
	if (exprhdl.Pdpscalar(1)->FHasSubquery())
	{
		return CXform::ExfpNone;
	}

	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2PartialDynamicIndexGet::Transform
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

	// extract components
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	// get the indexes on this relation
	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(pexprRelational->Pop());
	
	if (popGet->FPartial())
	{
		// already a partial dynamic get; do not try to split further
		return;
	}
	
	CTableDescriptor *ptabdesc = popGet->Ptabdesc();
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(ptabdesc->Pmdid());
	const ULONG ulIndices = pmdrel->UlIndices();

	if (0 == ulIndices)
	{
		// no indexes on the table
		return;
	}

	// array of expressions in the scalar expression
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	GPOS_ASSERT(0 < pdrgpexpr->UlLength());

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsScalarExpr = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();

	CColRefSet *pcrsReqd = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsReqd->Include(pcrsOutput);
	pcrsReqd->Include(pcrsScalarExpr);

	CPartConstraint *ppartcnstr = popGet->Ppartcnstr();
	ppartcnstr->AddRef();

	// find a candidate set of partial index combinations
	DrgPdrgPpartdig *pdrgpdrgppartdig = CXformUtils::PdrgpdrgppartdigCandidates
										(
										pmp,
										pmda,
										pdrgpexpr,
										popGet->PdrgpdrgpcrPart(),
										pmdrel,
										ppartcnstr,
										popGet->PdrgpcrOutput(),
										pcrsReqd,
										pcrsScalarExpr,
										NULL // pcrsAcceptedOuterRefs
										);
	
	// construct alternative partial index scan plans
	const ULONG ulCandidates = pdrgpdrgppartdig->UlLength();
	for (ULONG ul = 0; ul < ulCandidates; ul++)
	{
		DrgPpartdig *pdrgppartdig = (*pdrgpdrgppartdig)[ul];
		CreatePartialIndexGetPlan(pmp, pexpr, pdrgppartdig, pmdrel, pxfres);
	}

	ppartcnstr->Release();
	pcrsReqd->Release();
	pdrgpexpr->Release();
	pdrgpdrgppartdig->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::CreatePartialIndexGetPlan
//
//	@doc:
//		Create a plan as a union of the given partial index get candidates and 
//		possibly a dynamic table scan
//
//---------------------------------------------------------------------------
void
CXformSelect2PartialDynamicIndexGet::CreatePartialIndexGetPlan
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPpartdig *pdrgppartdig,
	const IMDRelation *pmdrel,
	CXformResult *pxfres
	)
	const
{ 
	CExpression *pexprRelational = (*pexpr)[0];
	CExpression *pexprScalar = (*pexpr)[1];

	CLogicalDynamicGet *popGet = CLogicalDynamicGet::PopConvert(pexprRelational->Pop());
	DrgPcr *pdrgpcrGet = popGet->PdrgpcrOutput();
	
	const ULONG ulPartialIndexes = pdrgppartdig->UlLength();
	
	DrgDrgPcr *pdrgpdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);
	DrgPexpr *pdrgpexprInput = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulPartialIndexes; ul++)
	{
		SPartDynamicIndexGetInfo *ppartdig = (*pdrgppartdig)[ul];

		const IMDIndex *pmdindex = ppartdig->m_pmdindex;
		CPartConstraint *ppartcnstr = ppartdig->m_ppartcnstr;
		DrgPexpr *pdrgpexprIndex = ppartdig->m_pdrgpexprIndex;
		DrgPexpr *pdrgpexprResidual = ppartdig->m_pdrgpexprResidual;
		
		DrgPcr *pdrgpcrNew = pdrgpcrGet;

		if (0 < ul)
		{
			pdrgpcrNew = CUtils::PdrgpcrCopy(pmp, pdrgpcrGet);
		}
		else
		{
			pdrgpcrNew->AddRef();
		}

		CExpression *pexprDynamicScan = NULL;
		if (NULL != pmdindex)
		{
			pexprDynamicScan = CXformUtils::PexprPartialDynamicIndexGet
								(
								pmp,
								popGet,
								pexpr->Pop()->UlOpId(),
								pdrgpexprIndex,
								pdrgpexprResidual,
								pdrgpcrNew,
								pmdindex,
								pmdrel,
								ppartcnstr,
								NULL, // pcrsAcceptedOuterRefs
								NULL, // pdrgpcrOuter
								NULL // pdrgpcrNewOuter
								);
		}
		else
		{
			pexprDynamicScan = PexprSelectOverDynamicGet
								(
								pmp,
								popGet,
								pexprScalar,
								pdrgpcrNew,
								ppartcnstr
								);
		}
		GPOS_ASSERT(NULL != pexprDynamicScan);

		pdrgpdrgpcrInput->Append(pdrgpcrNew);
		pdrgpexprInput->Append(pexprDynamicScan);
	}

	ULONG ulInput = pdrgpexprInput->UlLength();
	if (0 < ulInput)
	{
		CExpression *pexprResult = NULL;
		if (1 < ulInput)
		{
			pdrgpcrGet->AddRef();
			DrgPcr *pdrgpcrOuter = pdrgpcrGet;

			// construct a new union all operator
			pexprResult = GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CLogicalUnionAll(pmp, pdrgpcrOuter, pdrgpdrgpcrInput, popGet->UlScanId()),
							pdrgpexprInput
							);
		}
		else
		{
			pexprResult = (*pdrgpexprInput)[0];
			pexprResult->AddRef();

			// clean up
			pdrgpexprInput->Release();
			pdrgpdrgpcrInput->Release();
		}

		// if scalar expression involves the partitioning key, keep a SELECT node
		// on top for the purposes of partition selection
		DrgDrgPcr *pdrgpdrgpcrPartKeys = popGet->PdrgpdrgpcrPart();
		CExpression *pexprPredOnPartKey = CPredicateUtils::PexprExtractPredicatesOnPartKeys
											(
											pmp,
											pexprScalar,
											pdrgpdrgpcrPartKeys,
											NULL, /*pcrsAllowedRefs*/
											true /*fUseConstraints*/
											);
		
		if (NULL != pexprPredOnPartKey)
		{
			pexprResult = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalSelect(pmp), pexprResult, pexprPredOnPartKey);
		}
		
		pxfres->Add(pexprResult);

		return;
	}

	// clean up
	pdrgpdrgpcrInput->Release();
	pdrgpexprInput->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2PartialDynamicIndexGet::PexprSelectOverDynamicGet
//
//	@doc:
//		Create a partial dynamic get expression with a select on top
//
//---------------------------------------------------------------------------
CExpression *
CXformSelect2PartialDynamicIndexGet::PexprSelectOverDynamicGet
	(
	IMemoryPool *pmp,
	CLogicalDynamicGet *popGet,
	CExpression *pexprScalar,
	DrgPcr *pdrgpcrDGet,
	CPartConstraint *ppartcnstr
	)
{
	HMUlCr *phmulcr = CUtils::PhmulcrMapping(pmp, popGet->PdrgpcrOutput(), pdrgpcrDGet);

	// construct a partial dynamic get with the negated constraint
	CPartConstraint *ppartcnstrPartialDynamicGet = ppartcnstr->PpartcnstrCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);

	CLogicalDynamicGet *popPartialDynamicGet =
			(CLogicalDynamicGet *) popGet->PopCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/);
	popPartialDynamicGet->SetPartConstraint(ppartcnstrPartialDynamicGet);
	popPartialDynamicGet->SetSecondaryScanId(COptCtxt::PoctxtFromTLS()->UlPartIndexNextVal());
	popPartialDynamicGet->SetPartial();

	CExpression *pexprSelect = GPOS_NEW(pmp) CExpression
								(
								pmp,
								GPOS_NEW(pmp) CLogicalSelect(pmp),
								GPOS_NEW(pmp) CExpression(pmp, popPartialDynamicGet),
								pexprScalar->PexprCopyWithRemappedColumns(pmp, phmulcr, true /*fMustExist*/)
								);

	phmulcr->Release();

	return pexprSelect;
}

// EOF

