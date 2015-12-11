//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorDXLToExprTest.cpp
//
//	@doc:
//		Tests translating DXL trees into Expr tree.
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "gpos/error/CException.h"
#include "gpos/error/CMessage.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/test/CUnittest.h"

#include "unittest/base.h"
#include "unittest/gpopt/translate/CTranslatorDXLToExprTest.h"

// XSD location
#include "unittest/dxl/CParseHandlerTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/base/IDatum.h"
#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"

#include "gpopt/base/CUtils.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/operators/CLogicalGet.h"
#include "gpopt/operators/CLogicalSelect.h"
#include "gpopt/operators/CScalarConst.h"
#include "gpopt/operators/CScalarOp.h"
#include "gpopt/operators/CScalarIdent.h"
#include "gpopt/metadata/CColumnDescriptor.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"

#define GPDB_INT4_GT_OP OID(521)
#define GPDB_INT4_ADD_OP OID(551)

const CHAR *szQueryTableScan = "../data/dxl/expressiontests/TableScan.xml";
const CHAR *szQueryLimit = "../data/dxl/expressiontests/LimitQuery.xml";
const CHAR *szQueryLimitNoOffset = "../data/dxl/expressiontests/LimitQueryNoOffset.xml";
const CHAR *szQueryTVF = "../data/dxl/expressiontests/TableValuedFunctionQuery.xml";
static const CHAR *szQueryScalarSubquery = "../data/dxl/expressiontests/ScalarSubqueryQuery.xml";

static
const CHAR *
m_rgszDXLFileNames[] =
	{
		"../data/dxl/query/dxl-q17.xml",
		"../data/dxl/query/dxl-q18.xml",
		"../data/dxl/query/dxl-q19.xml",
		"../data/dxl/query/dxl-q23.xml",
	};
//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest
//
//	@doc:
//		Unittest for translating PlannedStmt to DXL
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest()
{
	CUnittest rgut[] =
		{
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_SingleTableQuery),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_SelectQuery),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithConst),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithBoolExpr),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithScalarOp),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_Limit),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_LimitNoOffset),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_ScalarSubquery),
				GPOS_UNITTEST_FUNC(CTranslatorDXLToExprTest::EresUnittest_TVF),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::Pexpr
//
//	@doc:
//		The function takes as input to the DXL file containing the plan, and a DXL file
//		containing the meta data, and returns its corresponding Expr Tree.
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExprTest::Pexpr
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName
	)
{
	// get the dxl document
	CHAR *szDXLExpected = CDXLUtils::SzRead(pmp, szDXLFileName);

	// parse the DXL tree from the given DXL document
	CQueryToDXLResult *ptroutput = CDXLUtils::PdxlnParseDXLQuery(pmp, szDXLExpected, NULL /*szXSDPath*/);
	
	GPOS_ASSERT(NULL != ptroutput->Pdxln() && NULL != ptroutput->PdrgpdxlnOutputCols());

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// translate DXL Tree -> Expr Tree
	CTranslatorDXLToExpr *pdxltr = GPOS_NEW(pmp) CTranslatorDXLToExpr(pmp, pmda);

	CExpression *pexprTranslated =	pdxltr->PexprTranslateQuery
												(
												ptroutput->Pdxln(),
												ptroutput->PdrgpdxlnOutputCols(),
												ptroutput->PdrgpdxlnCTE()
												);
	
	//clean up
	GPOS_DELETE(ptroutput);
	GPOS_DELETE(pdxltr);
	GPOS_DELETE_ARRAY(szDXLExpected);
	return pexprTranslated;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresTranslateAndCheck
//
//	@doc:
//		Main tool for testing translation of DXL trees into Expr. The function
//		takes as input to a DXL file containing the algebrized query, and a DXL file
//		containing the meta data. Next, translate the DXL into Expr Tree. Lastly,
//		compare the translated Expr Tree against the expected Expr Tree.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresTranslateAndCheck
	(
	IMemoryPool *pmp,
	const CHAR *szDXLFileName,
	const CWStringDynamic *pstrExpected
	)
{
	GPOS_RESULT eres = GPOS_FAILED;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	// translate the DXL document into Expr Tree
	CExpression *pexprTranslated = Pexpr(pmp, szDXLFileName);

	// get the string representation of the Expr Tree
	CWStringDynamic *pstrTranslated = Pstr(pmp, pexprTranslated);

	GPOS_ASSERT(NULL != pstrExpected && NULL != pstrTranslated);

	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	// compare the two Expr trees
	if (!pstrExpected->FEquals(pstrTranslated))
	{
		oss << "Output does not match expected DXL document" << std::endl;
		oss << "Expected: " << std::endl;
		oss << pstrExpected->Wsz() << std::endl;
		oss << "Actual: " << std::endl;
		oss << pstrTranslated->Wsz() << std::endl;
	}
	else
	{
		oss << "Output matches expected DXL document" << std::endl;
		eres = GPOS_OK;
	}

	GPOS_TRACE(str.Wsz());

	// clean up
	pexprTranslated->Release();
	GPOS_DELETE(pstrExpected);
	GPOS_DELETE(pstrTranslated);

	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::Pstr
//
//	@doc:
//		Generate a string representation of an Expr Tree
//
//---------------------------------------------------------------------------
CWStringDynamic *
CTranslatorDXLToExprTest::Pstr
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	CWStringDynamic str(pmp);
	COstreamString *poss = GPOS_NEW(pmp) COstreamString(&str);

	*poss << std::endl;
	pexpr->OsPrint(*poss);

	CWStringDynamic *pstrExpr = GPOS_NEW(pmp) CWStringDynamic(pmp, str.Wsz());

	GPOS_DELETE(poss);

	return pstrExpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::PexprGet
//
//	@doc:
//		Create a get expression for a table (r) with two integer columns a and b
//
//---------------------------------------------------------------------------
CExpression *
CTranslatorDXLToExprTest::PexprGet
	(
	IMemoryPool *pmp
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);

	CWStringConst strTblName(GPOS_WSZ_LIT("r"));

	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID21, 1, 1);

	CTableDescriptor *ptabdesc =
		GPOS_NEW(pmp) CTableDescriptor
					(
					pmp, 
					pmdid, 
					CName(&strTblName), 
					false, // fConvertHashToRandom
					CMDRelationGPDB::EreldistrMasterOnly, 
					CMDRelationGPDB::ErelstorageHeap, 
					0 // ulExecuteAsUser
					);

	// create an integer column (a) with column id 1
	CWStringConst strColA(GPOS_WSZ_LIT("a"));
	CColumnDescriptor *pcoldescIntA = GPOS_NEW(pmp) CColumnDescriptor(pmp, pmdtypeint4, CName(&strColA), 1, false /*FNullable*/);
	ptabdesc->AddColumn(pcoldescIntA);

	// create an integer column (b) with column id 2
	CWStringConst strColB(GPOS_WSZ_LIT("b"));
	CColumnDescriptor *pcoldescIntB = GPOS_NEW(pmp) CColumnDescriptor(pmp, pmdtypeint4, CName(&strColB), 2, false /*FNullable*/);
	ptabdesc->AddColumn(pcoldescIntB);

	// generate get expression
	CWStringConst strTblNameAlias(GPOS_WSZ_LIT("r"));

	return CTestUtils::PexprLogicalGet(pmp, ptabdesc, &strTblNameAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_SingleTableQuery
//
//	@doc:
//		Test translating a DXL Tree for the query (select * from r)
//		into Expr Tree
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_SingleTableQuery()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic *pstrExpected = NULL;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// manually create the Expr Tree
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);
		CExpression *pexprExpected = PexprGet(pmp);
		pstrExpected = Pstr(pmp, pexprExpected);

		//clean up
		pexprExpected->Release();
	}

	return EresTranslateAndCheck(pmp, szQueryTableScan, pstrExpected);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_TVF
//
//	@doc:
//		Test translating a DXL Tree for the query
//		(select * from generate_series(1, 10)) into Expr Tree
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_TVF()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic *pstrExpected = NULL;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// manually create the Expr Tree
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);
		CExpression *pexprExpected = CTestUtils::PexprLogicalTVFTwoArgs(pmp);
		pstrExpected = Pstr(pmp, pexprExpected);

		//clean up
		pexprExpected->Release();
	}

	return EresTranslateAndCheck(pmp, szQueryTVF, pstrExpected);
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_SelectQuery
//
//	@doc:
//		Test translating a DXL Tree for the query
//		(select * from r where a = b) into Expr Tree
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_SelectQuery()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic *pstrExpected = NULL;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// manually create the Expr Tree
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

		CExpression *pexprLgGet = PexprGet(pmp);

		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());


		// the output column references from the logical get
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();

		GPOS_ASSERT(NULL != pdrgpcr && 2 == pdrgpcr->UlSafeLength());

		CColRef *pcrLeft =  (*pdrgpcr)[0];
		CColRef *pcrRight = (*pdrgpcr)[1];

		CExpression *pexprPredicate = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);

		CExpression *pexprExpected =  CUtils::PexprLogicalSelect(pmp, pexprLgGet, pexprPredicate);
		pstrExpected = Pstr(pmp, pexprExpected);

		//clean up
		pexprExpected->Release();
	}

	return EresTranslateAndCheck(pmp, m_rgszDXLFileNames[0], pstrExpected);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithConst
//
//	@doc:
//		Test translating a DXL Tree for the query (select * from r where a = 5)
//		into Expr Tree
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithConst()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic *pstrExpected = NULL;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// manually create the Expr Tree
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);
		CExpression *pexprLgGet = PexprGet(pmp);
		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());

		// the output column references from the logical get
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();
		GPOS_ASSERT(NULL != pdrgpcr && 2 == pdrgpcr->UlSafeLength());

		CColRef *pcrLeft =  (*pdrgpcr)[0];
		ULONG ulVal = 5;
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(pmp, ulVal);
		CExpression *pexprScCmp = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pexprScConst);

		CExpression *pexprExpected = CUtils::PexprLogicalSelect(pmp, pexprLgGet, pexprScCmp);
		pstrExpected = Pstr(pmp, pexprExpected);

		//clean up
		pexprExpected->Release();
	}

	return EresTranslateAndCheck(pmp, m_rgszDXLFileNames[1], pstrExpected);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithBoolExpr
//
//	@doc:
//		Test translating a DXL Tree for the query
//		(select * from r where a = 5 and a = b) into Expr Tree
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithBoolExpr()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic *pstrExpected = NULL;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// manually create the Expr Tree
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

		CExpression *pexprLgGet = PexprGet(pmp);

		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());

		// the output column references from the logical get
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();

		GPOS_ASSERT(NULL != pdrgpcr && 2 == pdrgpcr->UlSafeLength());

		// create a scalar compare for a = 5
		CColRef *pcrLeft =  (*pdrgpcr)[0];

		ULONG ulVal = 5;
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(pmp, ulVal);

		CExpression *pexprScCmp = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pexprScConst);

		// create a scalar compare for a = b
		CColRef *pcrRight = (*pdrgpcr)[1];
		DrgPexpr *pdrgpexprInput = GPOS_NEW(pmp) DrgPexpr(pmp, 2);
		pdrgpexprInput->Append(pexprScCmp);
		pexprScCmp = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);

		pdrgpexprInput->Append(pexprScCmp);
		CExpression *pexprScBool = CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprInput);
		CExpression *pexprExpected = CUtils::PexprLogicalSelect(pmp, pexprLgGet, pexprScBool);
		pstrExpected = Pstr(pmp, pexprExpected);

		//clean up
		pexprExpected->Release();
	}

	return EresTranslateAndCheck(pmp, m_rgszDXLFileNames[2], pstrExpected);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithScalarOp
//
//	@doc:
//		Test translating a DXL Tree for the query
//		(select * from r where a > b + 2) into Expr Tree
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_SelectQueryWithScalarOp()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWStringDynamic *pstrExpected = NULL;

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// manually create the Expr Tree
	{
		// install opt context in TLS
		CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

		CExpression *pexprLgGet = PexprGet(pmp);

		CLogicalGet *popGet = CLogicalGet::PopConvert(pexprLgGet->Pop());

		// the output column references from the logical get
		DrgPcr *pdrgpcr = popGet->PdrgpcrOutput();

		GPOS_ASSERT(NULL != pdrgpcr && 2 == pdrgpcr->UlSafeLength());

		// create a scalar op (arithmetic) for b + 2
		CColRef *pcrLeft =  (*pdrgpcr)[1];

		ULONG ulVal = 2;
		CExpression *pexprScConst = CUtils::PexprScalarConstInt4(pmp, ulVal);
		CExpression *pexprScOp =
			CUtils::PexprScalarOp(pmp, pcrLeft, pexprScConst, CWStringConst(GPOS_WSZ_LIT("+")), GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_ADD_OP));

		// create a scalar compare for a > b + 2
		CColRef *pcrRight = (*pdrgpcr)[0];
		CExpression *pexprScCmp =
			CUtils::PexprScalarCmp(pmp, pcrRight, pexprScOp, CWStringConst(GPOS_WSZ_LIT(">")), GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_GT_OP));

		CExpression *pexprExpected = CUtils::PexprLogicalSelect(pmp, pexprLgGet, pexprScCmp);
		pstrExpected = Pstr(pmp, pexprExpected);

		//clean up
		pexprExpected->Release();
	}

	return EresTranslateAndCheck(pmp, m_rgszDXLFileNames[3], pstrExpected);
}


//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_Limit
//
//	@doc:
//		Test translating a DXL Tree for an order by-limit query
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_Limit()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	// translate the DXL document into Expr Tree
	CExpression *pexprTranslated = Pexpr(pmp, szQueryLimit);
	
	CWStringDynamic str(pmp);
	COstreamString *poss = GPOS_NEW(pmp) COstreamString(&str);

	*poss << std::endl;
	
	pexprTranslated->OsPrint(*poss);
	
	GPOS_TRACE(str.Wsz());
	
	pexprTranslated->Release();
	GPOS_DELETE(poss);
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_LimitNoOffset
//
//	@doc:
//		Test translating a DXL Tree for an order by-limit query with no offset specified
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_LimitNoOffset()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache(), CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	// translate the DXL document into Expr Tree
	CExpression *pexprTranslated = Pexpr(pmp, szQueryLimitNoOffset);
	
	CWStringDynamic str(pmp);
	COstreamString *poss = GPOS_NEW(pmp) COstreamString(&str);

	*poss << std::endl;
	
	pexprTranslated->OsPrint(*poss);
	
	GPOS_TRACE(str.Wsz());

	pexprTranslated->Release();
	GPOS_DELETE(poss);
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CTranslatorDXLToExprTest::EresUnittest_ScalarSubquery
//
//	@doc:
//		Test translating a DXL query with a scalar subquery
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTranslatorDXLToExprTest::EresUnittest_ScalarSubquery()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	// setup a file-based provider
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	CMDAccessor mda(pmp, CMDCache::Pcache());
	mda.RegisterProvider(CTestUtils::m_sysidDefault, pmdp);

	// install opt context in TLS
	CAutoOptCtxt aoc
					(
					pmp,
					&mda,
					NULL,  /* pceeval */
					CTestUtils::Pcm(pmp)
					);

	// translate the DXL document into Expr Tree
	CExpression *pexprTranslated = Pexpr(pmp, szQueryScalarSubquery);
	
	CWStringDynamic str(pmp);
	COstreamString *poss = GPOS_NEW(pmp) COstreamString(&str);

	*poss << std::endl;
	
	pexprTranslated->OsPrint(*poss);
	
	GPOS_TRACE(str.Wsz());

	pexprTranslated->Release();
	GPOS_DELETE(poss);
	return GPOS_OK;
}

// EOF
