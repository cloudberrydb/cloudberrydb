//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCNFConverterTest.cpp
//
//	@doc:
//		Test for CNF conversion
//---------------------------------------------------------------------------
#include "gpos/io/COstreamString.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/base/CQueryContext.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CCNFConverter.h"
#include "gpopt/operators/CPredicateUtils.h"
#include "gpopt/xforms/CXformUtils.h"

#include "unittest/base.h"
#include "unittest/gpopt/operators/CCNFConverterTest.h"
#include "unittest/gpopt/CTestUtils.h"

//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::EresUnittest
//
//	@doc:
//		Unittest driver
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCNFConverterTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		//GPOS_UNITTEST_FUNC(CCNFConverterTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CCNFConverterTest::EresUnittest_AvoidCNFConversion),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::EresUnittest_Basic
//
//	@doc:
//		Basic test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCNFConverterTest::EresUnittest_Basic()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	typedef CExpression *(*Pfpexpr)(IMemoryPool*);
	Pfpexpr rgpf[] =
		{
		CTestUtils::PexprLogicalSelectWithNestedAnd,
		CTestUtils::PexprLogicalSelectWithNestedOr,
		CTestUtils::PexprLogicalSelectWithEvenNestedNot,
		CTestUtils::PexprLogicalSelectWithOddNestedNot,
		CTestUtils::PexprLogicalSelectWithNestedAndOrNot
		};

	for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgpf); i++)
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
		CExpression *pexpr = rgpf[i](pmp);
		CExpression *pexprPreprocessed = CExpressionPreprocessor::PexprPreprocess(pmp, pexpr);

		CWStringDynamic str(pmp);
		COstreamString oss(&str);

		oss	<< std::endl << "SCALAR EXPR:" << std::endl << *(*pexpr)[1] << std::endl;
		GPOS_TRACE(str.Wsz());
		str.Reset();

		if (1 < pexprPreprocessed->UlArity())
		{
			CExpression *pexprCNF = CCNFConverter::Pexpr2CNF(pmp, (*pexprPreprocessed)[1]);
			oss	<< std::endl << "CNF REPRESENTATION:" << std::endl << *pexprCNF << std::endl;
			GPOS_TRACE(str.Wsz());
			str.Reset();
			pexprCNF->Release();
		}

		pexpr->Release();
		pexprPreprocessed->Release();
	}

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::EresUnittest_AvoidCNFConversion
//
//	@doc:
//		Unit test to check whether CNF conversion is done only when necessary
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCNFConverterTest::EresUnittest_AvoidCNFConversion()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	SCNConverterSTestCase rgpf[] =
	{
		{PexprScIdentCmpConst, NULL, false},
		{PexprOr, NULL, false},
		{PexprAnd1, NULL, false},
		{PexprAnd2, NULL, true},
		{PexprAnd3, NULL, false},
		{PexprAnd4, NULL, true},
		{PexprAnd5, NULL, false},
		{PexprAnd6, NULL, true},

		{PexprAnd1, PexprAnd1, false},
		{PexprAnd1, PexprAnd2, false},
		{PexprAnd1, PexprAnd3, false},
		{PexprAnd1, PexprAnd4, false},
		{PexprAnd1, PexprAnd5, false},
		{PexprAnd1, PexprAnd6, false},


		{PexprAnd2, PexprAnd1, false},
		{PexprAnd2, PexprAnd2, true},
		{PexprAnd2, PexprAnd3, false},
		{PexprAnd2, PexprAnd4, true},
		{PexprAnd2, PexprAnd5, false},
		{PexprAnd2, PexprAnd6, true},

		{PexprAnd3, PexprAnd1, false},
		{PexprAnd3, PexprAnd2, false},
		{PexprAnd3, PexprAnd3, false},
		{PexprAnd3, PexprAnd4, false},
		{PexprAnd3, PexprAnd5, false},
		{PexprAnd3, PexprAnd6, false},

		{PexprAnd4, PexprAnd1, false},
		{PexprAnd4, PexprAnd2, true},
		{PexprAnd4, PexprAnd3, false},
		{PexprAnd4, PexprAnd4, true},
		{PexprAnd4, PexprAnd5, false},
		{PexprAnd4, PexprAnd6, true},

		{PexprAnd5, PexprAnd1, false},
		{PexprAnd5, PexprAnd2, false},
		{PexprAnd5, PexprAnd3, false},
		{PexprAnd5, PexprAnd4, false},
		{PexprAnd5, PexprAnd5, false},
		{PexprAnd5, PexprAnd6, false},


		{PexprAnd6, PexprAnd1, false},
		{PexprAnd6, PexprAnd2, true},
		{PexprAnd6, PexprAnd3, false},
		{PexprAnd6, PexprAnd4, true},
		{PexprAnd6, PexprAnd5, false},
		{PexprAnd6, PexprAnd6, true},

		{PexprAnd7, PexprAnd7, true},

	};

	GPOS_RESULT eres = GPOS_OK;
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgpf) && (GPOS_OK == eres); ul++)
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
						(
						pmp,
						&mda,
						NULL,  /* pceeval */
						CTestUtils::Pcm(pmp)
						);

		SCNConverterSTestCase elem = rgpf[ul];
		PexprTestCase *pfFst = elem.m_pfFst;
		PexprTestCase *pfSnd = elem.m_pfSnd;

		CExpression *pexprLeft = CTestUtils::PexprLogicalGet(pmp);
		CExpression *pexprRight = CTestUtils::PexprLogicalGet(pmp);

		pexprLeft->PdpDerive();
		pexprRight->PdpDerive();

		GPOS_ASSERT(NULL != pfFst);
		CExpression *pexprScalar = NULL;
		if (NULL != pfSnd)
		{
			pexprScalar = CTestUtils::PexprOr(pmp, pfFst(pmp, pexprLeft, pexprRight), pfSnd(pmp, pexprLeft, pexprRight));
		}
		else
		{
			pexprScalar = pfFst(pmp, pexprLeft, pexprRight);
		}

		CExpression *pexprCNF = CXformUtils::Pexpr2CNFWhenBeneficial(pmp, pexprLeft, pexprRight, pexprScalar);

		{
			CAutoTrace at(pmp);
			at.Os() <<  std::endl << "SCALAR EXPR:" <<  std::endl <<  *pexprScalar << std::endl;
			at.Os() <<  std::endl << "CNF EXPR:" << std::endl << *pexprCNF << std::endl;
		}

		eres = GPOS_OK;
		if (elem.m_fCNFConversion && pexprScalar == pexprCNF)
		{
			eres = GPOS_FAILED;
		}

		if (!elem.m_fCNFConversion && pexprScalar != pexprCNF)
		{
			eres = GPOS_FAILED;
		}

		// clean up
		pexprLeft->Release();
		pexprRight->Release();
		pexprScalar->Release();
		pexprCNF->Release();
	}

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprScIdentCmpConst
//
//	@doc:
//		Scalar expression comparing scalar ident to a constant
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprScIdentCmpConst
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *//pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);

	return CTestUtils::PexprScIdentCmpConst
						(
						pmp,
						pexprLeft,
						IMDType::EcmptEq /* ecmpt */,
						10 /* ulVal */
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprOr
//
//	@doc:
//		OR of scalar expression comparing scalar ident to a constant
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprOr
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *//pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);

	CExpression *pexprFst = CTestUtils::PexprScIdentCmpConst
										(
										pmp,
										pexprLeft,
										IMDType::EcmptEq /* ecmpt */,
										10 /* ulVal */
										);

	CExpression *pexprSnd = CTestUtils::PexprScIdentCmpConst
										(
										pmp,
										pexprLeft,
										IMDType::EcmptEq /* ecmpt */,
										20 /* ulVal */
										);

	return CTestUtils::PexprOr(pmp, pexprFst, pexprSnd);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprScIdentCmpScIdent
//
//	@doc:
//		Scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprScIdentCmpScIdent
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);

	return CTestUtils::PexprScIdentCmpScIdent
						(
						pmp,
						pexprLeft,
						pexprRight,
						IMDType::EcmptEq /* ecmpt */
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd1
//
//	@doc:
//		AND of scalar expression comparing scalar ident to a constant
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd1
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *//pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);

	return CTestUtils::PexprAnd
						(
						pmp,
						CTestUtils::PexprScIdentCmpConst
									(
									pmp,
									pexprLeft,
									IMDType::EcmptEq /* ecmpt */,
									10 /* ulVal */
									),
						CTestUtils::PexprScIdentCmpConst
									(
									pmp,
									pexprLeft,
									IMDType::EcmptEq /* ecmpt */,
									20 /* ulVal */
									)
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd2
//
//	@doc:
//		AND of scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd2
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return CTestUtils::PexprAnd
						(
						pmp,
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */),
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */)
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd3
//
//	@doc:
//		AND of scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd3
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return CTestUtils::PexprAnd
						(
						pmp,
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptG /* ecmpt */),
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptL /* ecmpt */)
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd4
//
//	@doc:
//		AND of scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd4
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return  CTestUtils::PexprAnd
						(
						pmp,
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */),
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptL /* ecmpt */)
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd5
//
//	@doc:
//		AND of scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd5
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return  CTestUtils::PexprAnd
						(
						pmp,
						CTestUtils::PexprScIdentCmpConst
									(
									pmp,
									pexprLeft,
									IMDType::EcmptEq /* ecmpt */,
									10 /* ulVal */
									),
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptL /* ecmpt */)
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd6
//
//	@doc:
//		AND of scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd6
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	return  CTestUtils::PexprAnd
						(
						pmp,
						CTestUtils::PexprScIdentCmpConst
									(
									pmp,
									pexprLeft,
									IMDType::EcmptEq /* ecmpt */,
									10 /* ulVal */
									),
						CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */)
						);
}


//---------------------------------------------------------------------------
//	@function:
//		CCNFConverterTest::PexprAnd7
//
//	@doc:
//		AND of scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CCNFConverterTest::PexprAnd7
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);

	CExpression *pexprOr1 = CTestUtils::PexprOr
								(
								pmp,
								CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */),
								CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */)
								);

	CExpression *pexprOr2 = CTestUtils::PexprOr
								(
								pmp,
								CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */),
								CTestUtils::PexprScIdentCmpScIdent(pmp, pexprLeft, pexprRight, IMDType::EcmptEq /* ecmpt */)
								);

	return CTestUtils::PexprAnd(pmp, pexprOr1, pexprOr2);
}


// EOF
