//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDefaultComparator.cpp
//
//	@doc:
//		Default comparator for IDatum instances to be used in constraint derivation
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/memory/CAutoMemoryPool.h"

#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/COptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/eval/IConstExprEvaluator.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/base/IDatum.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDType.h"

using namespace gpopt;
using namespace gpos;
using gpnaucrates::IDatum;

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::CDefaultComparator
//
//	@doc:
//		Ctor
//		Does not take ownership of the constant expression evaluator
//
//---------------------------------------------------------------------------
CDefaultComparator::CDefaultComparator
	(
	IConstExprEvaluator *pceeval
	)
	:
	m_pceeval(pceeval)
{
	GPOS_ASSERT(NULL != pceeval);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::PexprEvalComparison
//
//	@doc:
//		Constructs a comparison expression of type ecmpt between the two given
//		data and evaluates it.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FEvalComparison
	(
	IMemoryPool *pmp,
	const IDatum *pdatum1,
	const IDatum *pdatum2,
	IMDType::ECmpType ecmpt
	)
	const
{
	GPOS_ASSERT(m_pceeval->FCanEvalExpressions());

	IDatum *pdatum1Copy = pdatum1->PdatumCopy(pmp);
	CExpression *pexpr1 = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum1Copy));
	IDatum *pdatum2Copy = pdatum2->PdatumCopy(pmp);
	CExpression *pexpr2 = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarConst(pmp, pdatum2Copy));
	CExpression *pexprComp = CUtils::PexprScalarCmp(pmp, pexpr1, pexpr2, ecmpt);

	CExpression *pexprResult = m_pceeval->PexprEval(pexprComp);
	pexprComp->Release();
	CScalarConst *popScalarConst = CScalarConst::PopConvert(pexprResult->Pop());
	IDatum *pdatum = popScalarConst->Pdatum();

	GPOS_ASSERT(IMDType::EtiBool == pdatum->Eti());
	IDatumBool *pdatumBool = dynamic_cast<IDatumBool *>(pdatum);
	BOOL fResult = pdatumBool->FValue();
	pexprResult->Release();

	return fResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::FEqual
//
//	@doc:
//		Tests if the two arguments are equal.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FEqual
	(
	const IDatum *pdatum1,
	const IDatum *pdatum2
	)
	const
{
	if (!CUtils::FConstrainableType(pdatum1->Pmdid()) ||
			!CUtils::FConstrainableType(pdatum2->Pmdid()))
	{

		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(pdatum1->Pmdid()) &&
			CUtils::FIntType(pdatum2->Pmdid()))
	{

		return pdatum1->FStatsEqual(pdatum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (pdatum1->FNull() && pdatum2->FNull())
	{
		return true;
	}

	return FEvalComparison(amp.Pmp(), pdatum1, pdatum2, IMDType::EcmptEq);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::FLessThan
//
//	@doc:
//		Tests if the first argument is less than the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FLessThan
	(
	const IDatum *pdatum1,
	const IDatum *pdatum2
	)
	const
{
	if (!CUtils::FConstrainableType(pdatum1->Pmdid()) ||
			!CUtils::FConstrainableType(pdatum2->Pmdid()))
	{

		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(pdatum1->Pmdid()) &&
			CUtils::FIntType(pdatum2->Pmdid()))
	{

		return pdatum1->FStatsLessThan(pdatum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (pdatum1->FNull() && !pdatum2->FNull())
	{
		return true;
	}
	
	return FEvalComparison(amp.Pmp(), pdatum1, pdatum2, IMDType::EcmptL);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::FLessThanOrEqual
//
//	@doc:
//		Tests if the first argument is less than or equal to the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FLessThanOrEqual
	(
	const IDatum *pdatum1,
	const IDatum *pdatum2
	)
	const
{
	if (!CUtils::FConstrainableType(pdatum1->Pmdid()) ||
			!CUtils::FConstrainableType(pdatum2->Pmdid()))
	{

		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(pdatum1->Pmdid()) &&
			CUtils::FIntType(pdatum2->Pmdid()))
	{

		return pdatum1->FStatsLessThan(pdatum2) || pdatum1->FStatsEqual(pdatum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (pdatum1->FNull())
	{
		// return true since either:
		// 1. pdatum1 is NULL and pdatum2 is not NULL. Since NULL is considered
		// less that not null values for interval computations we return true
		// 2. when pdatum1 and pdatum2 are both NULL so they are equal
		// for interval computation

		return true;
	}


	return FEvalComparison(amp.Pmp(), pdatum1, pdatum2, IMDType::EcmptLEq);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::FGreaterThan
//
//	@doc:
//		Tests if the first argument is greater than the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FGreaterThan
	(
	const IDatum *pdatum1,
	const IDatum *pdatum2
	)
	const
{
	if (!CUtils::FConstrainableType(pdatum1->Pmdid()) ||
			!CUtils::FConstrainableType(pdatum2->Pmdid()))
	{

		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(pdatum1->Pmdid()) &&
			CUtils::FIntType(pdatum2->Pmdid()))
	{

		return pdatum1->FStatsGreaterThan(pdatum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (!pdatum1->FNull() && pdatum2->FNull())
	{
		return true;
	}

	return FEvalComparison(amp.Pmp(), pdatum1, pdatum2, IMDType::EcmptG);
}

//---------------------------------------------------------------------------
//	@function:
//		CDefaultComparator::FLessThanOrEqual
//
//	@doc:
//		Tests if the first argument is greater than or equal to the second.
//
//---------------------------------------------------------------------------
BOOL
CDefaultComparator::FGreaterThanOrEqual
	(
	const IDatum *pdatum1,
	const IDatum *pdatum2
	)
	const
{
	if (!CUtils::FConstrainableType(pdatum1->Pmdid()) ||
			!CUtils::FConstrainableType(pdatum2->Pmdid()))
	{

		return false;
	}
	if (FUseBuiltinIntEvaluators() && CUtils::FIntType(pdatum1->Pmdid()) &&
			CUtils::FIntType(pdatum2->Pmdid()))
	{

		return pdatum1->FStatsGreaterThan(pdatum2) || pdatum1->FStatsEqual(pdatum2);
	}
	CAutoMemoryPool amp;

	// NULL datum is a special case and is being handled here. Assumptions made are
	// NULL is less than everything else. NULL = NULL.
	// Note : NULL is considered equal to NULL because we are using the comparator for
	//        interval calculation.
	if (pdatum2->FNull())
	{
		// return true since either:
		// 1. pdatum2 is NULL and pdatum1 is not NULL. Since NULL is considered
		// less that not null values for interval computations we return true
		// 2. when pdatum1 and pdatum2 are both NULL so they are equal
		// for interval computation
		return true;
	}

	return FEvalComparison(amp.Pmp(), pdatum1, pdatum2, IMDType::EcmptGEq);
}

// EOF
