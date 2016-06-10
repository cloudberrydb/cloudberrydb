//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDecorrelatorTest.cpp
//
//	@doc:
//		Test for decorrelation
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CDecorrelator.h"

#include "unittest/base.h"
#include "unittest/gpopt/xforms/CDecorrelatorTest.h"
#include "unittest/gpopt/CTestUtils.h"


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelatorTest::EresUnittest
//
//	@doc:
//		Unittest for predicate utilities
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDecorrelatorTest::EresUnittest()
{

	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CDecorrelatorTest::EresUnittest_Decorrelate),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CDecorrelatorTest::EresUnittest_Decorrelate
//
//	@doc:
//		Driver for decorrelation tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CDecorrelatorTest::EresUnittest_Decorrelate()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);
	
	// test cases
	typedef CExpression *(*Pfpexpr)(IMemoryPool*);
	Pfpexpr rgpf[] =
		{
		CTestUtils::PexprLogicalGbAggCorrelated,
		CTestUtils::PexprLogicalSelectCorrelated,
		CTestUtils::PexprLogicalJoinCorrelated,
		CTestUtils::PexprLogicalProjectGbAggCorrelated
		};

	for (ULONG ulCase = 0; ulCase < GPOS_ARRAY_SIZE(rgpf); ulCase++)
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

		// generate expression
		CExpression *pexpr = rgpf[ulCase](pmp);

		CWStringDynamic str(pmp);
		COstreamString oss(&str);
		oss	<< std::endl << "INPUT:" << std::endl << *pexpr << std::endl;
		GPOS_TRACE(str.Wsz());
		str.Reset();

		CExpression *pexprResult = NULL;
		DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
#ifdef GPOS_DEBUG
		BOOL fSuccess = 
#endif // GPOS_DEBUG
		CDecorrelator::FProcess(pmp, pexpr, false /*fEqualityOnly*/, &pexprResult, pdrgpexpr);
		GPOS_ASSERT(fSuccess);
		
		// convert residuals into one single conjunct
		CExpression *pexprResidual = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);

		oss	<< std::endl << "RESIDUAL RELATIONAL:" << std::endl << *pexprResult << std::endl;
		oss	<< std::endl << "RESIDUAL SCALAR:" << std::endl << *pexprResidual << std::endl;

		GPOS_TRACE(str.Wsz());
		str.Reset();

		pexprResult->Release();
		pexprResidual->Release();
		pexpr->Release();
	}

	return GPOS_OK;
}

// EOF
