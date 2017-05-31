//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformImplementCorrelatedApply.h
//
//	@doc:
//		Base class for implementing correlated NLJ
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformImplementCorrelatedApply_H
#define GPOPT_CXformImplementCorrelatedApply_H

#include "gpos/base.h"
#include "gpopt/xforms/CXformImplementation.h"
#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformImplementCorrelatedApply
	//
	//	@doc:
	//		Implement correlated Apply
	//
	//---------------------------------------------------------------------------
	template<class TLogicalApply, class TPhysicalJoin>
	class CXformImplementCorrelatedApply : public CXformImplementation
	{

		private:

			// private copy ctor
			CXformImplementCorrelatedApply(const CXformImplementCorrelatedApply &);
		

		public:

			// ctor
			explicit
			CXformImplementCorrelatedApply(IMemoryPool *pmp)
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

			// dtor
			virtual
			~CXformImplementCorrelatedApply()
			{}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp(CExpressionHandle &) const
            {
                return CXform::ExfpHigh;
            }

			// actual transform
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
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

	}; // class CXformImplementCorrelatedApply

}

#endif // !GPOPT_CXformImplementCorrelatedApply_H

// EOF
