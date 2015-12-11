//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformSelect2IndexGet.cpp
//
//	@doc:
//		Implementation of select over a table to an index get transformation
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformSelect2IndexGet.h"
#include "gpopt/xforms/CXformUtils.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDIndexGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2IndexGet::CXformSelect2IndexGet
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformSelect2IndexGet::CXformSelect2IndexGet
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
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalGet(pmp)), // relational child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))	// predicate tree
				)
		)
{}


//---------------------------------------------------------------------------
//	@function:
//		CXformSelect2IndexGet::Exfp
//
//	@doc:
//		Compute xform promise for a given expression handle
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformSelect2IndexGet::Exfp
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
//		CXformSelect2IndexGet::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformSelect2IndexGet::Transform
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
	CLogicalGet *popGet = CLogicalGet::PopConvert(pexprRelational->Pop());
	const ULONG ulIndices = popGet->Ptabdesc()->UlIndices();
	if (0 == ulIndices)
	{
		return;
	}
	
	// array of expressions in the scalar expression
	DrgPexpr *pdrgpexpr = CPredicateUtils::PdrgpexprConjuncts(pmp, pexprScalar);
	GPOS_ASSERT(pdrgpexpr->UlLength() > 0);

	// derive the scalar and relational properties to build set of required columns
	CColRefSet *pcrsOutput = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsScalarExpr = CDrvdPropScalar::Pdpscalar(pexprScalar->PdpDerive())->PcrsUsed();

	CColRefSet *pcrsReqd = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrsReqd->Include(pcrsOutput);
	pcrsReqd->Include(pcrsScalarExpr);

	// find the indexes whose included columns meet the required columns
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	const IMDRelation *pmdrel = pmda->Pmdrel(popGet->Ptabdesc()->Pmdid());

	for (ULONG ul = 0; ul < ulIndices; ul++)
	{
		IMDId *pmdidIndex = pmdrel->PmdidIndex(ul);
		const IMDIndex *pmdindex = pmda->Pmdindex(pmdidIndex);
		CExpression *pexprIndexGet = CXformUtils::PexprLogicalIndexGet
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
						 NULL /*ppartcnstrIndex*/
						);
		if (NULL != pexprIndexGet)
		{
			pxfres->Add(pexprIndexGet);
		}
	}

	pcrsReqd->Release();
	pdrgpexpr->Release();
}

// EOF

