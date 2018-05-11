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
	IMemoryPool *mp
	)
	:
	// pattern
	CXformImplementation
		(
		GPOS_NEW(mp) CExpression
				(
				mp,
				GPOS_NEW(mp) CLogicalIndexGet(mp),
				GPOS_NEW(mp) CExpression(mp, GPOS_NEW(mp) CPatternLeaf(mp))	// index lookup predicate
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
	IMemoryPool *mp = pxfctxt->Pmp();

	CIndexDescriptor *pindexdesc = pop->Pindexdesc();
	pindexdesc->AddRef();

	CTableDescriptor *ptabdesc = pop->Ptabdesc();
	ptabdesc->AddRef();

	CColRefArray *pdrgpcrOutput = pop->PdrgpcrOutput();
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
		GPOS_NEW(mp) CExpression
			(
			mp,
			GPOS_NEW(mp) CPhysicalIndexScan
				(
				mp,
				pindexdesc,
				ptabdesc,
				pexpr->Pop()->UlOpId(),
				GPOS_NEW(mp) CName (mp, pop->NameAlias()),
				pdrgpcrOutput,
				pos
				),
			pexprIndexCond
			);
	pxfres->Add(pexprAlt);
}


// EOF

