//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTestUtils.inl
//
//	@doc:
//		Implementation of inlined functions;
//---------------------------------------------------------------------------
#ifndef GPOPT_CTestUtils_INL
#define GPOPT_CTestUtils_INL

//#include "unittest/gpopt/CTestUtils.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//		CTestUtils::PexprJoinPartitionedOuter
	//
	//	@doc:
	//		Generate a randomized join expression over a partitioned table
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprJoinPartitionedOuter
		(
		IMemoryPool *pmp
		)
	{
		return PexprJoinPartitioned<T>(pmp, true /*fOuterPartitioned*/);
	}
	
	//---------------------------------------------------------------------------
	//		CTestUtils::PexprJoinPartitionedInner
	//
	//	@doc:
	//		Generate a randomized join expression over a partitioned table
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprJoinPartitionedInner
		(
		IMemoryPool *pmp
		)
	{
		return PexprJoinPartitioned<T>(pmp, false /*fOuterPartitioned*/);
	}
	
	//---------------------------------------------------------------------------
	//		CTestUtils::PexprJoinPartitioned
	//
	//	@doc:
	//		Generate a randomized join expression over a partitioned table
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprJoinPartitioned
		(
		IMemoryPool *pmp,
		BOOL fOuterPartitioned
		)
	{
		// create an outer Get expression and extract a random column from it
		CExpression *pexprGet = PexprLogicalGet(pmp);
		CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
		CColRef *pcr =  pcrs->PcrAny();

		// create an inner partitioned Get expression
		CExpression *pexprGetPartitioned = PexprLogicalGetPartitioned(pmp);
		
		// extract first partition key
		CLogicalGet *popGetPartitioned = CLogicalGet::PopConvert(pexprGetPartitioned->Pop());
		const DrgDrgPcr *pdrgpdrgpcr = popGetPartitioned->PdrgpdrgpcrPartColumns();

		GPOS_ASSERT(pdrgpdrgpcr != NULL);
		GPOS_ASSERT(0 < pdrgpdrgpcr->UlLength());
		DrgPcr *pdrgpcr = (*pdrgpdrgpcr)[0];
		GPOS_ASSERT(1 == pdrgpcr->UlLength());
		CColRef *pcrPartKey = (*pdrgpcr)[0];
		
		// construct a comparison pk = a
		CExpression *pexprScalar = CUtils::PexprScalarEqCmp
											(
											pmp,
											CUtils::PexprScalarIdent(pmp, pcr),
											CUtils::PexprScalarIdent(pmp, pcrPartKey)
											);
		
		if (fOuterPartitioned)
		{
			return CUtils::PexprLogicalJoin<T>(pmp, pexprGetPartitioned, pexprGet, pexprScalar);
		}
		
		return CUtils::PexprLogicalJoin<T>(pmp, pexprGet, pexprGetPartitioned, pexprScalar);
	}
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CTestUtils::PexprLogicalJoin
	//
	//	@doc:
	//		Generate a join expression with a random equality predicate
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprLogicalJoin
		(
		IMemoryPool *pmp,
		CExpression *pexprLeft,
		CExpression *pexprRight
		)
	{
		GPOS_ASSERT(NULL != pexprLeft);
		GPOS_ASSERT(NULL != pexprRight);

		// get any two columns; one from each side
		CDrvdPropRelational *pdprelLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive());
		CColRef *pcrLeft = pdprelLeft->PcrsOutput()->PcrAny();

		CDrvdPropRelational *pdprelRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive());
		CColRef *pcrRight = pdprelRight->PcrsOutput()->PcrAny();
		CExpression *pexprEquality = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);

		return CUtils::PexprLogicalJoin<T>(pmp, pexprLeft, pexprRight, pexprEquality);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CTestUtils::PexprLogicalJoin
	//
	//	@doc:
	//		Generate randomized basic join expression
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprLogicalJoin
		(
		IMemoryPool *pmp
		)
	{
		CExpression *pexprLeft = PexprLogicalGet(pmp);
		CExpression *pexprRight = PexprLogicalGet(pmp);
		// reversing the order here is a workaround for non-determinism
		// in CExpressionPreprocessorTest where join order was flipped
		// depending on the compiler used
		return PexprLogicalJoin<T>(pmp, pexprRight, pexprLeft);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CTestUtils::PexprLogicalApply
	//
	//	@doc:
	//		Generate top-level apply
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprLogicalApply
		(
		IMemoryPool *pmp
		)
	{
		CExpression *pexprOuter = PexprLogicalGet(pmp);
		CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	
		CExpression *pexprInner = PexprLogicalSelectCorrelated(pmp, pcrsOuter, 3);
		CExpression *pexprPredicate = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
		
		COperator *pop = GPOS_NEW(pmp) T(pmp);
		return GPOS_NEW(pmp) CExpression(pmp, pop, pexprOuter, pexprInner, pexprPredicate);
	}

	//---------------------------------------------------------------------------
	//	@function:
	//		CTestUtils::PexprLogicalApplyWithOuterRef
	//
	//	@doc:
	//		Generate top-level apply
	//
	//---------------------------------------------------------------------------
	template<class T>
	CExpression *
	CTestUtils::PexprLogicalApplyWithOuterRef
		(
		IMemoryPool *pmp
		)
	{
		CExpression *pexprOuter = PexprLogicalGet(pmp);
		CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	
		CExpression *pexprInner = PexprLogicalSelectCorrelated(pmp, pcrsOuter, 3);
		CExpression *pexprPredicate = CUtils::PexprScalarConstBool(pmp, true /*fVal*/);
		
		CColRefSet *pcrsOuterRef = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOuter();
		GPOS_ASSERT(1 == pcrsOuterRef->CElements());
		CColRef *pcr = pcrsOuterRef->PcrFirst();

		DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
		pdrgpcr->Append(pcr);
		COperator *pop = GPOS_NEW(pmp) T(pmp, pdrgpcr, COperator::EopScalarSubquery);
		return GPOS_NEW(pmp) CExpression(pmp, pop, pexprOuter, pexprInner, pexprPredicate);
	}

} // namespace gpopt

#endif // !GPOPT_CTestUtils_INL

// EOF
