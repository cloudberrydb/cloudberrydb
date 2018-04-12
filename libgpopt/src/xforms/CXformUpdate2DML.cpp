//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUpdate2DML.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformUpdate2DML.h"
#include "gpopt/xforms/CXformUtils.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/optimizer/COptimizerConfig.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::CXformUpdate2DML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformUpdate2DML::CXformUpdate2DML
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
				GPOS_NEW(pmp) CLogicalUpdate(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::Exfp
//
//	@doc:
//		Compute promise of xform
//
//---------------------------------------------------------------------------
CXform::EXformPromise
CXformUpdate2DML::Exfp
	(
	CExpressionHandle & // exprhdl
	)
	const
{
	return CXform::ExfpHigh;
}

//---------------------------------------------------------------------------
//	@function:
//		CXformUpdate2DML::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformUpdate2DML::Transform
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

	CLogicalUpdate *popUpdate = CLogicalUpdate::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	// extract components for alternative

	CTableDescriptor *ptabdesc = popUpdate->Ptabdesc();
	DrgPcr *pdrgpcrDelete = popUpdate->PdrgpcrDelete();
	DrgPcr *pdrgpcrInsert = popUpdate->PdrgpcrInsert();
	CColRef *pcrCtid = popUpdate->PcrCtid();
	CColRef *pcrSegmentId = popUpdate->PcrSegmentId();
	CColRef *pcrTupleOid = popUpdate->PcrTupleOid();
	
	// child of update operator
	CExpression *pexprChild = (*pexpr)[0];
	pexprChild->AddRef();
	
	IMDId *pmdidRel = ptabdesc->Pmdid();
	if (CXformUtils::FTriggersExist(CLogicalDML::EdmlUpdate, ptabdesc, true /*fBefore*/))
	{
		pmdidRel->AddRef();
		pdrgpcrDelete->AddRef();
		pdrgpcrInsert->AddRef();
		pexprChild = CXformUtils::PexprRowTrigger
							(
							pmp,
							pexprChild,
							CLogicalDML::EdmlUpdate,
							pmdidRel,
							true /*fBefore*/,
							pdrgpcrDelete,
							pdrgpcrInsert
							);
	}

	// generate the action column and split operator
	COptCtxt *poctxt = COptCtxt::PoctxtFromTLS();
	CMDAccessor *pmda = poctxt->Pmda();
	CColumnFactory *pcf = poctxt->Pcf();

	pdrgpcrDelete->AddRef();
	pdrgpcrInsert->AddRef();

	const IMDType *pmdtype = pmda->PtMDType<IMDTypeInt4>();
	CColRef *pcrAction = pcf->PcrCreate(pmdtype, IDefaultTypeModifier, OidInvalidCollation);
	
	CExpression *pexprProjElem = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarProjectElement(pmp, pcrAction),
											GPOS_NEW(pmp) CExpression
														(
														pmp,
														GPOS_NEW(pmp) CScalarDMLAction(pmp)
														)
											);
	
	CExpression *pexprProjList = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarProjectList(pmp),
											pexprProjElem
											);
	CExpression *pexprSplit =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalSplit(pmp,	pdrgpcrDelete, pdrgpcrInsert, pcrCtid, pcrSegmentId, pcrAction, pcrTupleOid),
			pexprChild,
			pexprProjList
			);

	// add assert checking that no NULL values are inserted for nullable columns or no check constraints are violated
	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();
	CExpression *pexprAssertConstraints;
	if (poconf->Phint()->FEnforceConstraintsOnDML())
	{
		pexprAssertConstraints = CXformUtils::PexprAssertConstraints
			(
			pmp,
			pexprSplit,
			ptabdesc,
			pdrgpcrInsert
			);
	}
	else
	{
		pexprAssertConstraints = pexprSplit;
	}

	// generate oid column and project operator
	CExpression *pexprProject = NULL;
	CColRef *pcrTableOid = NULL;
	if (ptabdesc->FPartitioned())
	{
		// generate a partition selector
		pexprProject = CXformUtils::PexprLogicalPartitionSelector(pmp, ptabdesc, pdrgpcrInsert, pexprAssertConstraints);
		pcrTableOid = CLogicalPartitionSelector::PopConvert(pexprProject->Pop())->PcrOid();
	}
	else
	{
		// generate a project operator
		IMDId *pmdidTable = ptabdesc->Pmdid();

		OID oidTable = CMDIdGPDB::PmdidConvert(pmdidTable)->OidObjectId();
		CExpression *pexprOid = CUtils::PexprScalarConstOid(pmp, oidTable);

		pexprProject = CUtils::PexprAddProjection(pmp, pexprAssertConstraints, pexprOid);

		CExpression *pexprPrL = (*pexprProject)[1];
		pcrTableOid = CUtils::PcrFromProjElem((*pexprPrL)[0]);
	}
	
	GPOS_ASSERT(NULL != pcrTableOid);

	const ULONG ulCols = pdrgpcrInsert->UlLength();

	CBitSet *pbsModified = GPOS_NEW(pmp) CBitSet(pmp, ptabdesc->UlColumns());
	for (ULONG ul = 0; ul < ulCols; ul++)
	{
		CColRef *pcrInsert = (*pdrgpcrInsert)[ul];
		CColRef *pcrDelete = (*pdrgpcrDelete)[ul];
		if (pcrInsert != pcrDelete)
		{
			// delete columns refer to the original tuple's descriptor, if it's different 
			// from the corresponding insert column, then we're modifying the column
			// at that position
			pbsModified->FExchangeSet(ul);
		}
	}
	// create logical DML
	ptabdesc->AddRef();
	pdrgpcrInsert->AddRef();
	CExpression *pexprDML =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalDML(pmp, CLogicalDML::EdmlUpdate, ptabdesc, pdrgpcrInsert, pbsModified, pcrAction, pcrTableOid, pcrCtid, pcrSegmentId, pcrTupleOid),
			pexprProject
			);
	
	// TODO:  - Oct 30, 2012; detect and handle AFTER triggers on update
	
	pxfres->Add(pexprDML);

}

// EOF
