//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CUtils.inl
//
//	@doc:
//		Implementation of inlined functions;
//---------------------------------------------------------------------------
#ifndef GPOPT_CUtils_INL
#define GPOPT_CUtils_INL

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalJoin
	//
	//	@doc:
	//		Generate a join expression from given expressions
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalJoin
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		CExpression *pexprPredicate
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pexprPredicate);

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp),
				pexprLeft,
				pexprRight,
				pexprPredicate
				);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate an apply expression from given expressions
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		
		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}


	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate an apply expression with a known inner column
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		const CColRef *pcrInner,
		COperator::EOperatorId eopidOriginSubq,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pcrInner);
		
		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcr->Append(const_cast<CColRef *>(pcrInner));
		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp, pdrgpcr, eopidOriginSubq),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate an apply expression with known array of inner columns
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		DrgPcr *pdrgpcrInner,
		COperator::EOperatorId eopidOriginSubq,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pdrgpcrInner);
		GPOS_ASSERT(0 < pdrgpcrInner->UlLength());
		
		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}

		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp, pdrgpcrInner, eopidOriginSubq),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}

	//---------------------------------------------------------------------------
	//	@class:
	//		CUtils::PexprLogicalCorrelatedQuantifiedApply
	//
	//	@doc:
	//		Helper to create a left semi correlated apply from left semi apply
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CUtils::PexprLogicalCorrelatedQuantifiedApply
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight,
		DrgPcr *pdrgpcrInner,
		COperator::EOperatorId eopidOriginSubq,
		CExpression *pexprPred
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);
		GPOS_ASSERT(NULL != pdrgpcrInner);
		GPOS_ASSERT(0 < pdrgpcrInner->UlLength());
		
		CExpression *pexprScalar = pexprPred;
		if (NULL == pexprPred)
		{
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}
	
		if (COperator::EopLogicalSelect != pexprRight->Pop()->Eopid())
		{
			// quantified comparison was pushed down, we create a dummy comparison here
			GPOS_ASSERT(!CUtils::FHasOuterRefs(pexprRight) &&
				"unexpected outer references in inner child of Semi Apply expression ");
			pexprScalar->Release();
			pexprScalar = PexprScalarConstBool(pmp, true /*fVal*/);
		}
		else
		{
			// quantified comparison is now on top of inner expression, skip to child
			(*pexprRight)[1]->AddRef();
			CExpression *pexprNewPredicate = (*pexprRight)[1];
			pexprScalar->Release();
			pexprScalar = pexprNewPredicate;
			
			(*pexprRight)[0]->AddRef();
			CExpression *pexprChild = (*pexprRight)[0];
			pexprRight->Release();
			pexprRight = pexprChild;
		}
				
		return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) T(pmp, pdrgpcrInner, eopidOriginSubq),
				pexprLeft,
				pexprRight,
				pexprScalar
				);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::AddRefAppend
	//
	//	@doc:
	//		Append elements from input array to output array, starting from
	//		given index, after add-refing them
	//
	//---------------------------------------------------------------------------
	template <class T, void (*pfnDestroy)(T*)>
	void
	CUtils::AddRefAppend
		(
		CDynamicPtrArray<T, pfnDestroy> *pdrgptOutput,
		CDynamicPtrArray<T, pfnDestroy> *pdrgptInput,
		ULONG ulStart
		)
	{
		GPOS_ASSERT(NULL != pdrgptOutput);
		GPOS_ASSERT(NULL != pdrgptInput);
		
		const ULONG ulSize = pdrgptInput->UlLength();
		GPOS_ASSERT_IMP(0 < ulSize, ulStart < ulSize);
		
		for (ULONG ul = ulStart; ul < ulSize; ul++)
		{
			T *pt = (*pdrgptInput)[ul];
			CRefCount *prc = dynamic_cast<CRefCount *>(pt);
			prc->AddRef();
			pdrgptOutput->Append(pt);
		}
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FScalarConstInt
	//
	//	@doc:
	//		Check if the given expression is an INT,
	//		the template parameter is an INT type 
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FScalarConstInt
		(
		CExpression *pexpr
		)
	{
		GPOS_ASSERT(NULL != pexpr);
		
		IMDType::ETypeInfo eti = T::EtiType();
		GPOS_ASSERT(IMDType::EtiInt2 == eti || IMDType::EtiInt4 == eti || IMDType::EtiInt8 == eti);
		
		COperator *pop = pexpr->Pop();
		if (COperator::EopScalarConst == pop->Eopid())
		{
			CScalarConst *popScalarConst = CScalarConst::PopConvert(pop);
			if (eti == popScalarConst->Pdatum()->Eti())
			{
				return true;
			}
		}

		return false;
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchIndex
	//
	//	@doc:
	//		Match function between index get/scan operators 
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchIndex
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}
	
		T *popIndex = T::PopConvert(pop2);
	
		return pop1->UlOriginOpId() == popIndex->UlOriginOpId() &&
				pop1->Ptabdesc()->Pmdid()->FEquals(popIndex->Ptabdesc()->Pmdid()) &&
				pop1->Pindexdesc()->Pmdid()->FEquals(popIndex->Pindexdesc()->Pmdid()) &&
				pop1->PdrgpcrOutput()->FEqual(popIndex->PdrgpcrOutput());
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchDynamicIndex
	//
	//	@doc:
	//		Match function between dynamic index get/scan operators 
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchDynamicIndex
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}
		
		T *popIndex2 = T::PopConvert(pop2);
	
		// match if the index descriptors are identical
		// we will compare MDIds, so both indexes should be partial or non-partial.
		// for heterogeneous indexes, we use pointer comparison of part constraints to avoid
		// memory allocation because matching function is used while holding spin locks.
		// this means that we may miss matches for heterogeneous indexes
		return  pop1->UlOriginOpId() == popIndex2->UlOriginOpId() &&
				pop1->UlScanId() == popIndex2->UlScanId() &&
				pop1->UlSecondaryScanId() == popIndex2->UlSecondaryScanId() &&
				pop1->Ptabdesc()->Pmdid()->FEquals(popIndex2->Ptabdesc()->Pmdid()) &&
				pop1->Pindexdesc()->Pmdid()->FEquals(popIndex2->Pindexdesc()->Pmdid()) &&
				pop1->PdrgpcrOutput()->FEqual(popIndex2->PdrgpcrOutput()) &&
				(!pop1->FPartial() || (pop1->Ppartcnstr() == popIndex2->Ppartcnstr()));	
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchDynamicScan
	//
	//	@doc:
	//		Match function between dynamic get/scan operators 
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchDynamicScan
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}
		
		T *popScan2 = T::PopConvert(pop2);
	
		// match if the table descriptors are identical
		// for partial scans, we use pointer comparison of part constraints to avoid
		// memory allocation because matching function is used while holding spin locks.
		// this means that we may miss matches for partial scans
		return pop1->UlScanId() == popScan2->UlScanId() &&
				pop1->UlSecondaryScanId() == popScan2->UlSecondaryScanId() &&
				pop1->Ptabdesc()->Pmdid()->FEquals(popScan2->Ptabdesc()->Pmdid()) &&
				pop1->PdrgpcrOutput()->FEqual(popScan2->PdrgpcrOutput()) &&
				((!pop1->FPartial() && !popScan2->FPartial()) ||
				 (pop1->Ppartcnstr() == popScan2->Ppartcnstr()));	
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchDynamicBitmapScan
	//
	//	@doc:
	//		Match function between dynamic bitmap get/scan operators 
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchDynamicBitmapScan
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}
		
		T *popDynamicBitmapScan2 = T::PopConvert(pop2);
		
		return
			pop1->UlOriginOpId() == popDynamicBitmapScan2->UlOriginOpId() &&
			FMatchDynamicScan(pop1, pop2);	// call match dynamic scan to compare other member vars
	}
	
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CUtils::FMatchBitmapScan
	//
	//	@doc:
	//		Match function between bitmap get/scan operators 
	//
	//---------------------------------------------------------------------------
	template<class T>
	BOOL
	CUtils::FMatchBitmapScan
		(
		T *pop1,
		COperator *pop2
		)
	{
		if (pop1->Eopid() != pop2->Eopid())
		{
			return false;
		}

		T *popScan2 = T::PopConvert(pop2);

		return pop1->UlOriginOpId() == popScan2->UlOriginOpId() &&
			pop1->Ptabdesc()->Pmdid()->FEquals(popScan2->Ptabdesc()->Pmdid()) &&
			pop1->PdrgpcrOutput()->FEqual(popScan2->PdrgpcrOutput());
	}
}


#endif // !GPOPT_CUtils_INL

// EOF

