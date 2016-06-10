//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformIndexGet2IndexScan.cpp
//
//	@doc:
//		Implementation of transform
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpopt/xforms/CXformIndexGet2IndexScan.h"

#include "gpopt/operators/ops.h"
#include "gpopt/metadata/CIndexDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"

using namespace gpopt;


//---------------------------------------------------------------------------
//	@function:
//		CXformIndexGet2IndexScan::CXformIndexGet2IndexScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CXformIndexGet2IndexScan::CXformIndexGet2IndexScan
	(
	IMemoryPool *pmp
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalIndexGet(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp))	// index lookup predicate
				)
		)
{}

//---------------------------------------------------------------------------
//	@function:
//		CXformIndexGet2IndexScan::Transform
//
//	@doc:
//		Actual transformation
//
//---------------------------------------------------------------------------
void
CXformIndexGet2IndexScan::Transform
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

	CLogicalIndexGet *pop = CLogicalIndexGet::PopConvert(pexpr->Pop());
	IMemoryPool *pmp = pxfctxt->Pmp();

	CIndexDescriptor *pindexdesc = pop->Pindexdesc();
	pindexdesc->AddRef();

	CTableDescriptor *ptabdesc = pop->Ptabdesc();
	ptabdesc->AddRef();

	DrgPcr *pdrgpcrOutput = pop->PdrgpcrOutput();
	GPOS_ASSERT(NULL != pdrgpcrOutput);
	pdrgpcrOutput->AddRef();

	COrderSpec *pos = pop->Pos();
	GPOS_ASSERT(NULL != pos);
	pos->AddRef();

	// extract components
	CExpression *pexprIndexCond = (*pexpr)[0];

	// addref all children
	pexprIndexCond->AddRef();

	CExpression *pexprAlt =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CPhysicalIndexScan
				(
				pmp,
				pindexdesc,
				ptabdesc,
				pexpr->Pop()->UlOpId(),
				GPOS_NEW(pmp) CName (pmp, pop->NameAlias()),
				pdrgpcrOutput,
				pos
				),
			pexprIndexCond
			);
	pxfres->Add(pexprAlt);
}


// EOF

