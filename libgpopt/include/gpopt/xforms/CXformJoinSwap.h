//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformJoinSwap.h
//
//	@doc:
//		Join swap transformation
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoinSwap_H
#define GPOPT_CXformJoinSwap_H

#include "gpos/base.h"

#include "gpopt/operators/ops.h"

#include "gpopt/xforms/CXformExploration.h"
#include "gpopt/xforms/CXformUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CXformJoinSwap
	//
	//	@doc:
	//		Join swap transformation
	//
	//---------------------------------------------------------------------------
	template<class TJoinTop, class TJoinBottom>
	class CXformJoinSwap : public CXformExploration
	{

		private:

			// private copy ctor
			CXformJoinSwap(const CXformJoinSwap &);

		public:

			// ctor
			explicit
			CXformJoinSwap(IMemoryPool *pmp)
                :
                CXformExploration
                (
                 // pattern
                 GPOS_NEW(pmp) CExpression
                        (
                         pmp,
                         GPOS_NEW(pmp) TJoinTop(pmp),
                         GPOS_NEW(pmp) CExpression  // left child is a join tree
                                (
                                 pmp,
                                 GPOS_NEW(pmp) TJoinBottom(pmp),
                                 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // left child
                                 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // right child
                                 GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // predicate
                                 ),
                         GPOS_NEW(pmp) CExpression // right child is a pattern leaf
                                (
                                 pmp,
                                 GPOS_NEW(pmp) CPatternLeaf(pmp)
                                 ),
                         GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)) // top-join predicate
                         )
                )
            {}

			// dtor
			virtual
			~CXformJoinSwap() {}

			// compute xform promise for a given expression handle
			virtual
			EXformPromise Exfp
				(
				CExpressionHandle & // exprhdl
				)
				const
			{
				return CXform::ExfpHigh;
			}

			// actual transform
			void Transform
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

                CExpression *pexprResult = CXformUtils::PexprSwapJoins(pmp, pexpr, (*pexpr)[0]);
                if (NULL == pexprResult)
                {
                    return;
                }

                pxfres->Add(pexprResult);
            }

	}; // class CXformJoinSwap

}

#endif // !GPOPT_CXformJoinSwap_H

// EOF
