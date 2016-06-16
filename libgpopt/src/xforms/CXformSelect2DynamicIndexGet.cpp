//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2DynamicIndexGet.cpp
//
//	@doc:
//		Implementation of select over a partitioned table to a dynamic index get
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/metadata/CPartConstraint.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformSelect2DynamicIndexGet.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDIndexGPDB.h"
#include "naucrates/md/IMDPartConstraint.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2DynamicIndexGet::CXformSelect2DynamicIndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2DynamicIndexGet::CXformSelect2DynamicIndexGet
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
//		CXformSelect2DynamicIndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2DynamicIndexGet::Exfp
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
//		CXformSelect2DynamicIndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2DynamicIndexGet::Transform
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
	CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(pexprRelational->Pop());
	const ULONG ulIndices = popDynamicGet->Ptabdesc()->UlIndices();
	if (0 == ulIndices)
	{
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

	// find the indexes whose included columns meet the required columns
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(popDynamicGet->Ptabdesc()->Pmdid());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->PmdidIndex(ul);
		const IMDIndex *pmdindex = pmda->Pmdindex(pmdidIndex);
		CPartConstraint *ppartcnstrIndex = CUtils::PpartcnstrFromMDPartCnstr
								(
								pmp,
								COptCtxt::PoctxtFromTLS()->Pmda(),
								popDynamicGet->PdrgpdrgpcrPart(),
								pmdindex->Pmdpartcnstr(),
								popDynamicGet->PdrgpcrOutput()
								);
		CExpression *pexprDynamicIndexGet = CXformUtils::PexprLogicalIndexGet
							(
							pmp,
							pmda,
							pexprRelational,
							pexpr->Pop()->UlOpId(),
							pdrgpexpr,
							pcrsReqd,
							pcrsScalarExpr,
							NULL /*pcrsOuterRefs*/,
							pmdindex,
							pmdrel,
							false /*fAllowPartialIndex*/,
							ppartcnstrIndex
							);
		if (NULL != pexprDynamicIndexGet)
		{
			// create a redundant SELECT on top of DynamicIndexGet to be able to use predicate in partition elimination

			CExpression *pexprRedundantSelect = CXformUtils::PexprRedundantSelectForDynamicIndex(pmp, pexprDynamicIndexGet);
			pexprDynamicIndexGet->Release();
			pxfres->Add(pexprRedundantSelect);
		}
	}

	pcrsReqd->Release();
	pdrgpexpr->Release();
}

// EOF

