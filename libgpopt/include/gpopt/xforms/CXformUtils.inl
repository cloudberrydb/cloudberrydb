//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CXformUtils.inl
//
//	@doc:
//		Implementation of inlined xform utilities;
//---------------------------------------------------------------------------
#ifndef GPOPT_CXformUtils_INL
#define GPOPT_CXformUtils_INL

#include "naucrates/md/CMDIdGPDB.h"

#include "gpopt/base/CPartIndexMap.h"
#include "gpopt/search/CGroupProxy.h"

namespace gpopt
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@function:
	//		CXformUtils::TransformImplementBinaryOp
	//
	//	@doc:
	//		Helper function for implementation xforms on binary operators
	//		with predicates (e.g. joins)
	//
	//---------------------------------------------------------------------------
	template<class T>
	void
	CXformUtils::TransformImplementBinaryOp
		(
		CXformContext *pxfctxt,
		CXformResult *pxfres,
		CExpression *pexpr
		) 
	{
		GPOS_ASSERT(NULL != pxfctxt);
		GPOS_ASSERT(NULL != pexpr);

		IMemoryPool *pmp = pxfctxt->Pmp();

		// extract components
		CExpression *pexprLeft = (*pexpr)[0];
		CExpression *pexprRight = (*pexpr)[1];
		CExpression *pexprScalar = (*pexpr)[2];

		// addref all children
		pexprLeft->AddRef();
		pexprRight->AddRef();
		pexprScalar->AddRef();

		// assemble physical operator
		CExpression *pexprBinary =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp),
				pexprLeft,
				pexprRight,
				pexprScalar
				);

#ifdef GPOS_DEBUG
		COperator::EOperatorId eopid = pexprBinary->Pop()->Eopid();
#endif // GPOS_DEBUG
		GPOS_ASSERT
			(
			COperator::EopPhysicalInnerNLJoin == eopid ||
			COperator::EopPhysicalLeftOuterNLJoin == eopid ||
			COperator::EopPhysicalLeftSemiNLJoin == eopid ||
			COperator::EopPhysicalLeftAntiSemiNLJoin == eopid ||
			COperator::EopPhysicalLeftAntiSemiNLJoinNotIn == eopid
			);

		// add alternative to results
		pxfres->Add(pexprBinary);
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CXformUtils::AddHashJoinAlternative
	//
	//	@doc:
	//		Helper function for adding hash join alternative to given xform
	///		results
	//
	//---------------------------------------------------------------------------	
	template <class T>
	void 
	CXformUtils::AddHashJoinAlternative
		(		
		IMemoryPool *pmp,
		CExpression *pexprJoin,
		DrgPexpr *pdrgpexprOuter,
		DrgPexpr *pdrgpexprInner,
		CXformResult *pxfres
		)
	{
		GPOS_ASSERT(CUtils::FLogicalJoin(pexprJoin->Pop()));
		GPOS_ASSERT(3 == pexprJoin->UlArity());
		GPOS_ASSERT(NULL != pdrgpexprOuter);
		GPOS_ASSERT(NULL != pdrgpexprInner);
		GPOS_ASSERT(NULL != pxfres);
		
		for (ULONG ul = 0; ul < 3; ul++)
		{
			(*pexprJoin)[ul]->AddRef();
		}
		CExpression *pexprResult = GPOS_NEW(pmp) CExpression(pmp,
														GPOS_NEW(pmp) T(pmp, pdrgpexprOuter, pdrgpexprInner),
														(*pexprJoin)[0],
														(*pexprJoin)[1],
														(*pexprJoin)[2]);
		pxfres->Add(pexprResult);
	}
	

	//---------------------------------------------------------------------------
	//	@function:
	//		CXformUtils::ImplementHashJoin
	//
	//	@doc:
	//		Helper function for implementation of hash joins
	//
	//---------------------------------------------------------------------------	
	template <class T>
	void 
	CXformUtils::ImplementHashJoin
		(		
		CXformContext *pxfctxt,
		CXformResult *pxfres,
		CExpression *pexpr,
		BOOL fAntiSemiJoin // is the target hash join type an anti-semi join?
		)
	{
		GPOS_ASSERT(NULL != pxfctxt);
		
		// if there are outer references, then we cannot build a hash join
		if (CUtils::FHasOuterRefs(pexpr))
		{
			return;
		}

		IMemoryPool *pmp = pxfctxt->Pmp();
		DrgPexpr *pdrgpexprOuter = NULL;
		DrgPexpr *pdrgpexprInner = NULL;
		
		// check if we have already computed hash join keys for the scalar child
		LookupHashJoinKeys(pmp, pexpr, &pdrgpexprOuter, &pdrgpexprInner);
		if (NULL != pdrgpexprOuter)
		{
			GPOS_ASSERT(NULL != pdrgpexprInner);
			if (0 == pdrgpexprOuter->UlLength())
			{
				GPOS_ASSERT(0 == pdrgpexprInner->UlLength());
				
				// we failed before to find hash join keys for scalar child, 
				// no reason to try to do the same again
				pdrgpexprOuter->Release();
				pdrgpexprInner->Release();
			}
			else
			{
				// we have computed hash join keys on scalar child before, reuse them
				AddHashJoinAlternative<T>(pmp, pexpr, pdrgpexprOuter, pdrgpexprInner, pxfres);
			}
			
			return;
		}
		
		// first time to compute hash join keys on scalar child
				
		pdrgpexprOuter = GPOS_NEW(pmp) DrgPexpr(pmp);
		pdrgpexprInner = GPOS_NEW(pmp) DrgPexpr(pmp);
								
		CExpression *pexprInnerJoin = NULL;
		BOOL fHashJoinPossible = CPhysicalJoin::FHashJoinPossible(pmp, pexpr, pdrgpexprOuter, pdrgpexprInner, &pexprInnerJoin);
		
		// cache hash join keys on scalar child group
		CacheHashJoinKeys(pexprInnerJoin, pdrgpexprOuter, pdrgpexprInner);
		
		if (fHashJoinPossible)
		{
			AddHashJoinAlternative<T>(pmp, pexprInnerJoin, pdrgpexprOuter, pdrgpexprInner, pxfres);
		}
		else
		{
			// clean up
			pdrgpexprOuter->Release();
			pdrgpexprInner->Release();
		}
		
		pexprInnerJoin->Release();
		
		if (!fHashJoinPossible && fAntiSemiJoin)
		{
			CExpression *pexprProcessed = NULL;
			if (FProcessGPDBAntiSemiHashJoin(pmp, pexpr, &pexprProcessed))
			{
				// try again after simplifying join predicate
				ImplementHashJoin<T>(pxfctxt, pxfres, pexprProcessed, false /*fAntiSemiJoin*/);
				pexprProcessed->Release();
			}
		}
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CXformUtils::ImplementNLJoin
	//
	//	@doc:
	//		Helper function for implementation of nested loops joins
	//
	//---------------------------------------------------------------------------	
	template <class T>
	void
	CXformUtils::ImplementNLJoin
		(
		CXformContext *pxfctxt,
		CXformResult *pxfres,
		CExpression *pexpr
		)
	{
		GPOS_ASSERT(NULL != pxfctxt);
		
		IMemoryPool *pmp = pxfctxt->Pmp();
	
		DrgPcr *pdrgpcrOuter = GPOS_NEW(pmp) DrgPcr(pmp);
		DrgPcr *pdrgpcrInner = GPOS_NEW(pmp) DrgPcr(pmp);
		
		TransformImplementBinaryOp<T>(pxfctxt, pxfres, pexpr);

		// clean up
		pdrgpcrOuter->Release();
		pdrgpcrInner->Release();
	}
		
}

#endif // !GPOPT_CXformUtils_INL

// EOF
