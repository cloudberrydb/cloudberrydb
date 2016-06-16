//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCorrelatedApply.inl
//
//	@doc:
//		Template implementation of transformer
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementCorrelatedApply_INL
#define GPOPT_CXformImplementCorrelatedApply_INL

#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformImplementCorrelatedApply<TLogicalApply, TJoin>::CXformImplementCorrelatedApply
	//
	//	@doc:
	//		ctor
	//
	//---------------------------------------------------------------------------
	template<class TLogicalApply, class TPhysicalJoin>
	CXformImplementCorrelatedApply<TLogicalApply, TPhysicalJoin>::CXformImplementCorrelatedApply
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
				GPOS_NEW(pmp) TLogicalApply(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // predicate
				)
			)
	{}
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CXformImplementCorrelatedApply<TLogicalApply, TPhysicalJoin>::Exfp
	//
	//	@doc:
	//		Compute xform promise for a given expression handle;
	//
	//---------------------------------------------------------------------------
	template<class TLogicalApply, class TPhysicalJoin>
	CXform::EXformPromise CXformImplementCorrelatedApply<TLogicalApply, TPhysicalJoin>::Exfp
		(
		CExpressionHandle & // exprhdl
		)
		const
	{
		return CXform::ExfpHigh;
	}
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CXformImplementCorrelatedApply<TLogicalApply, TPhysicalJoin>::Transform
	//
	//	@doc:
	//		Transform logical correlated Apply into Physical correlated Apply
	//
	//---------------------------------------------------------------------------
	template<class TLogicalApply, class TPhysicalJoin>
	void CXformImplementCorrelatedApply<TLogicalApply, TPhysicalJoin>::Transform
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
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprRight = (*pexpr)[1];
		CExpression *pexprScalar = (*pexpr)[2];
		TLogicalApply *popApply = TLogicalApply::PopConvert(pexpr->Pop());
		DrgPcr *pdrgpcr = popApply->PdrgPcrInner();
		
		pdrgpcr->AddRef();

		// addref all children
		pexprLeft->AddRef();
		pexprRight->AddRef();
		pexprScalar->AddRef();

		// assemble physical operator
		CExpression *pexprPhysicalApply = 
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) TPhysicalJoin(pmp, pdrgpcr, popApply->EopidOriginSubq()),
				pexprLeft,
				pexprRight,
				pexprScalar
				);

		// add alternative to results
		pxfres->Add(pexprPhysicalApply);
	}
}

#endif // !GPOPT_CXformImplementCorrelatedApply_INL

// EOF
