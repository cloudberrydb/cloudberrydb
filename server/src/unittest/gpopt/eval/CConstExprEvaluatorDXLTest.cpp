//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CConstExprEvaluatorDXLTest.cpp
//
//	@doc:
//		Unit tests for CConstExprEvaluatorDXL
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLScalarConstValue.h"

#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/eval/CConstExprEvaluatorDXL.h"
#include "gpopt/exception.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpression.h"

#include "unittest/gpopt/eval/CConstExprEvaluatorDXLTest.h"
#include "unittest/base.h"
#include "unittest/gpopt/CTestUtils.h"

using namespace gpdxl;
using namespace gpnaucrates;
using namespace gpopt;

// Value  which the dummy constant evaluator should produce.
const INT CConstExprEvaluatorDXLTest::m_iDefaultEvalValue = 300;

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXLTest::CDummyConstDXLNodeEvaluator::PdxlnEvaluateExpr
//
//	@doc:
//		Evaluate the given DXL node representing an expression and returns a dummy
//		value as DXL. Caller must release it.
//
//---------------------------------------------------------------------------
gpdxl::CDXLNode *
CConstExprEvaluatorDXLTest::CDummyConstDXLNodeEvaluator::PdxlnEvaluateExpr
	(
	const gpdxl::CDXLNode * /*pdxlnExpr*/
	)
{
	const IMDTypeInt4 *pmdtypeint4 = m_pmda->PtMDType<IMDTypeInt4>();
	pmdtypeint4->Pmdid()->AddRef();

	CDXLDatumInt4 *pdxldatum = GPOS_NEW(m_pmp) CDXLDatumInt4(m_pmp, pmdtypeint4->Pmdid(), false /*fConstNull*/, m_iVal);
	CDXLScalarConstValue *pdxlnConst = GPOS_NEW(m_pmp) CDXLScalarConstValue(m_pmp, pdxldatum);

	return GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlnConst);
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXLTest::EresUnittest
//
//	@doc:
//		Executes all unit tests for CConstExprEvaluatorDXL
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstExprEvaluatorDXLTest::EresUnittest()
{
	{
		CUnittest rgut[] =
			{
			GPOS_UNITTEST_FUNC(CConstExprEvaluatorDXLTest::EresUnittest_Constants),
			GPOS_UNITTEST_FUNC_THROW
				(
				CConstExprEvaluatorDXLTest::EresUnittest_NonScalar,
				gpdxl::ExmaGPOPT,
				gpdxl::ExmiEvalUnsupportedScalarExpr
				),
			GPOS_UNITTEST_FUNC_THROW
				(
				CConstExprEvaluatorDXLTest::EresUnittest_NestedSubquery,
				gpdxl::ExmaGPOPT,
				gpdxl::ExmiEvalUnsupportedScalarExpr
				),
			GPOS_UNITTEST_FUNC_THROW
				(
				CConstExprEvaluatorDXLTest::EresUnittest_ScalarContainingVariables,
				gpdxl::ExmaGPOPT,
				gpdxl::ExmiEvalUnsupportedScalarExpr
				),
			};

		return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
	}

}


//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXLTest::EresUnittest_Constants
//
//	@doc:
//		Test evaluation for a constant.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstExprEvaluatorDXLTest::EresUnittest_Constants()
{
	CTestUtils::CTestSetup testsetup;
	IMemoryPool *pmp = testsetup.Pmp();
	CDummyConstDXLNodeEvaluator consteval(pmp, testsetup.Pmda(), m_iDefaultEvalValue);
	CConstExprEvaluatorDXL  *pceeval =
			GPOS_NEW(pmp) CConstExprEvaluatorDXL(pmp, testsetup.Pmda(), &consteval);

	// Test that evaluation works for an integer constant
	CExpression *pexprConstInput = CUtils::PexprScalarConstInt4(testsetup.Pmp(), m_iDefaultEvalValue);
	CExpression *pexprConstResult = pceeval->PexprEval(pexprConstInput);
#ifdef GPOS_DEBUG
	CScalarConst *popScConstInput = CScalarConst::PopConvert(pexprConstInput->Pop());
	CScalarConst *popScConstResult = CScalarConst::PopConvert(pexprConstResult->Pop());
	GPOS_ASSERT(popScConstResult->FMatch(popScConstInput));
#endif // GPOS_DEBUG
	pexprConstInput->Release();
	pexprConstResult->Release();
	pceeval->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXLTest::EresUnittest_NonScalar
//
//	@doc:
//		Test that evaluation fails for a non scalar input.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstExprEvaluatorDXLTest::EresUnittest_NonScalar()
{
	CTestUtils::CTestSetup testsetup;
	IMemoryPool *pmp = testsetup.Pmp();
	CDummyConstDXLNodeEvaluator consteval(pmp, testsetup.Pmda(), m_iDefaultEvalValue);
	CConstExprEvaluatorDXL  *pceeval =
			GPOS_NEW(pmp) CConstExprEvaluatorDXL(pmp, testsetup.Pmda(), &consteval);

	CExpression *pexprGet = CTestUtils::PexprLogicalGet(testsetup.Pmp());

	// this call should raise an exception
	CExpression *pexprResult = pceeval->PexprEval(pexprGet);
	CRefCount::SafeRelease(pexprResult);
	pexprGet->Release();
	pceeval->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXLTest::EresUnittest_NestedSubquery
//
//	@doc:
//		Test that an error is raised for a scalar with a nested subquery.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CConstExprEvaluatorDXLTest::EresUnittest_NestedSubquery()
{
	CTestUtils::CTestSetup testsetup;
	IMemoryPool *pmp = testsetup.Pmp();
	CDummyConstDXLNodeEvaluator consteval(pmp, testsetup.Pmda(), m_iDefaultEvalValue);
	CConstExprEvaluatorDXL  *pceeval =
			GPOS_NEW(pmp) CConstExprEvaluatorDXL(pmp, testsetup.Pmda(), &consteval);

	CExpression *pexprSelect = CTestUtils::PexprLogicalSelectWithConstAnySubquery(testsetup.Pmp());
	CExpression *pexprPredicate = (*pexprSelect)[1];
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == pexprPredicate->Pop()->Eopid());

	// this call should raise an exception
	CExpression *pexprResult = pceeval->PexprEval(pexprPredicate);
	CRefCount::SafeRelease(pexprResult);
	pexprSelect->Release();
	pceeval->Release();

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CConstExprEvaluatorDXLTest::EresUnittest_ScalarContainingVariables
//
//	@doc:
//		Test that an error is raised for a scalar containing variables.
//
//---------------------------------------------------------------------------
GPOS_RESULT CConstExprEvaluatorDXLTest::EresUnittest_ScalarContainingVariables()
{
	CTestUtils::CTestSetup testsetup;
	IMemoryPool *pmp = testsetup.Pmp();
	CDummyConstDXLNodeEvaluator consteval(pmp, testsetup.Pmda(), m_iDefaultEvalValue);
	CConstExprEvaluatorDXL  *pceeval =
			GPOS_NEW(pmp) CConstExprEvaluatorDXL(pmp, testsetup.Pmda(), &consteval);

	const IMDTypeInt4 *pmdtypeint4 = testsetup.Pmda()->PtMDType<IMDTypeInt4>();
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRef *pcrComputed = pcf->PcrCreate(pmdtypeint4, IDefaultTypeModifier, OidInvalidCollation);

	// create a comparison, where one of the children is a variable
	CExpression *pexprFunCall = CUtils::PexprScalarEqCmp
			(
			testsetup.Pmp(),
			CUtils::PexprScalarConstInt4(testsetup.Pmp(), 200 /*iVal*/),
			CUtils::PexprScalarIdent(testsetup.Pmp(), pcrComputed)
			);

	// this call should raise an exception
	CExpression *pexprResult = pceeval->PexprEval(pexprFunCall);
	CRefCount::SafeRelease(pexprResult);
	pexprFunCall->Release();
	pceeval->Release();

	return GPOS_OK;
}

// EOF
