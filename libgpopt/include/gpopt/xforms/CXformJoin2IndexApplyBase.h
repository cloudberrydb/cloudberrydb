//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software Inc.
//
//	Base class for transforming Join to Index Apply
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformJoin2IndexApplyBase_H
#define GPOPT_CXformJoin2IndexApplyBase_H

#include "gpos/base.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformJoin2IndexApply.h"

namespace gpopt
{
	using namespace gpos;

	template<class TJoin, class TApply, class TGet, BOOL fWithSelect, BOOL fPartial, IMDIndex::EmdindexType eidxtype>
	class CXformJoin2IndexApplyBase : public CXformJoin2IndexApply
	{
		private:

			// private copy ctor
			CXformJoin2IndexApplyBase(const CXformJoin2IndexApplyBase &);
			
		protected:

			virtual
			CLogicalJoin *PopLogicalJoin(IMemoryPool *pmp) const
			{
				return GPOS_NEW(pmp) TJoin(pmp);
			}


			virtual
			CLogicalApply *PopLogicalApply
				(
				IMemoryPool *pmp,
				DrgPcr *pdrgpcr
				) const
			{
				return GPOS_NEW(pmp) TApply(pmp, pdrgpcr);
			}

		public:

			// ctor
			explicit
			CXformJoin2IndexApplyBase<TJoin, TApply, TGet, fWithSelect, fPartial, eidxtype>(IMemoryPool *pmp)
			:
			// pattern
			CXformJoin2IndexApply
			(
				GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) TJoin(pmp),
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternLeaf(pmp)), // outer child
					fWithSelect
					?
					GPOS_NEW(pmp) CExpression  // inner child with Select operator
						(
						pmp,
						GPOS_NEW(pmp) CLogicalSelect(pmp),
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) TGet(pmp)), // Get below Select
						GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate
						)
					:
					GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) TGet(pmp)), // inner child with Get operator,
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CPatternTree(pmp))  // predicate tree
				)
			)
			{}

			// dtor
			virtual
			~CXformJoin2IndexApplyBase<TJoin, TApply, TGet, fWithSelect, fPartial, eidxtype>()
			{}

			// actual transform
			virtual
			void Transform(CXformContext *pxfctxt, CXformResult *pxfres, CExpression *pexpr) const
			{
				GPOS_ASSERT(NULL != pxfctxt);
				GPOS_ASSERT(FPromising(pxfctxt->Pmp(), this, pexpr));
				GPOS_ASSERT(FCheckPattern(pexpr));

				IMemoryPool *pmp = pxfctxt->Pmp();

				// extract components
				CExpression *pexprOuter = (*pexpr)[0];
				CExpression *pexprInner = (*pexpr)[1];
				CExpression *pexprScalar = (*pexpr)[2];

				CExpression *pexprGet = pexprInner;
				CExpression *pexprAllPredicates = pexprScalar;

				if (fWithSelect)
				{
					pexprGet = (*pexprInner)[0];
					pexprAllPredicates = CPredicateUtils::PexprConjunction(pmp, pexprScalar, (*pexprInner)[1]);
				}
				else
				{
					pexprScalar->AddRef();
				}

				CLogicalDynamicGet *popDynamicGet = NULL;
				if (COperator::EopLogicalDynamicGet == pexprGet->Pop()->Eopid())
				{
					popDynamicGet = CLogicalDynamicGet::PopConvert(pexprGet->Pop());
				}

				CTableDescriptor *ptabdescInner = TGet::PopConvert(pexprGet->Pop())->Ptabdesc();
				if (fPartial)
				{
					CreatePartialIndexApplyAlternatives
						(
						pmp,
						pexpr->Pop()->UlOpId(),
						pexprOuter,
						pexprInner,
						pexprAllPredicates,
						ptabdescInner,
						popDynamicGet,
						pxfres
						);
				}
				else
				{
					CreateHomogeneousIndexApplyAlternatives
						(
						pmp,
						pexpr->Pop()->UlOpId(),
						pexprOuter,
						pexprGet,
						pexprAllPredicates,
						ptabdescInner,
						popDynamicGet,
						pxfres,
						eidxtype
						);
				}
				CRefCount::SafeRelease(pexprAllPredicates);
			}

	}; // class CXformJoin2IndexApplyBase

}

#endif // !GPOPT_CXformJoin2IndexApplyBase_H

// EOF
