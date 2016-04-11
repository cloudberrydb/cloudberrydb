//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTestUtils.cpp
//
//	@doc:
//		Implementation of test utility functions
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CMessage.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "gpopt/base/CConstraintInterval.h"
#include "gpopt/base/CCTEMap.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CDefaultComparator.h"
#include "gpopt/base/CDistributionSpecHashed.h"
#include "gpopt/eval/CConstExprEvaluatorDefault.h"
#include "gpopt/eval/IConstExprEvaluator.h"
#include "gpopt/operators/ops.h"
#include "gpopt/operators/CScalarSubqueryAll.h"
#include "gpopt/operators/CScalarSubqueryAny.h"
#include "gpopt/xforms/CSubqueryHandler.h"

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/IMDTypeInt2.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/IMDScalarOp.h"
#include "naucrates/base/CDatumInt2GPDB.h"
#include "naucrates/base/CDatumInt4GPDB.h"
#include "naucrates/base/CDatumInt8GPDB.h"
#include "naucrates/base/CDatumBoolGPDB.h"

#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"

#include "gpopt/exception.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/base/CColRefSetIter.h"
#include "gpopt/base/CPrintPrefix.h"
#include "gpopt/engine/CEngine.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/mdcache/CMDCache.h"
#include "gpopt/minidump/CMinidumperUtils.h"
#include "gpopt/operators/ops.h"
#include "gpopt/xforms/CXformUtils.h"
#include "gpopt/translate/CTranslatorDXLToExpr.h"
#include "gpopt/translate/CTranslatorExprToDXL.h"

#include "naucrates/base/CQueryToDXLResult.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "unittest/base.h"
#include "unittest/gpopt/CSubqueryTestUtils.h"
#include "unittest/gpopt/CTestUtils.h"

#define GPOPT_SEGMENT_COUNT 2 // number segments for testing

using namespace gpopt;

// static variable initialization
// default source system id
CSystemId 
CTestUtils::m_sysidDefault(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));


// XSD path
const CHAR *
CTestUtils::m_szXSDPath = "http://greenplum.com/dxl/2010/12/ dxl.xsd";

// metadata file
const CHAR *
CTestUtils::m_szMDFileName = "../data/dxl/metadata/md.xml";

// provider file
CMDProviderMemory *
CTestUtils::m_pmdpf = NULL;

// local memory pool
IMemoryPool *
CTestUtils::m_pmp = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::CTestSetup::PmdpSetupFileBasedProvider
//
//	@doc:
//		set up a file based provider
//
//---------------------------------------------------------------------------
CMDProviderMemory *
CTestUtils::CTestSetup::PmdpSetupFileBasedProvider()
{
	CMDProviderMemory *pmdp = CTestUtils::m_pmdpf;
	pmdp->AddRef();
	return pmdp;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::CTestSetup::CTestSetup
//
//	@doc:
//		ctor
//
//---------------------------------------------------------------------------
CTestUtils::CTestSetup::CTestSetup()
	:
	m_amp(),
	m_mda(m_amp.Pmp(), CMDCache::Pcache(), CTestUtils::m_sysidDefault, PmdpSetupFileBasedProvider()),
	// install opt context in TLS
	m_aoc
		(
		m_amp.Pmp(),
		&m_mda,
		NULL,  /* pceeval */
		CTestUtils::Pcm(m_amp.Pmp())
		)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::InitProviderFile
//
//	@doc:
//		Initialize provider file;
//		called before unittests start
//
//---------------------------------------------------------------------------
void
CTestUtils::InitProviderFile
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pmp);
	GPOS_ASSERT(NULL != pmp);

	m_pmp = pmp;
	m_pmdpf = GPOS_NEW(m_pmp) CMDProviderMemory(m_pmp, m_szMDFileName);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::DestroyMDProvider
//
//	@doc:
//		Destroy metadata file;
//		called after all unittests complete
//
//---------------------------------------------------------------------------
void
CTestUtils::DestroyMDProvider()
{
	GPOS_ASSERT(NULL != m_pmp);

	CRefCount::SafeRelease(m_pmdpf);

	// release local memory pool
	CMemoryPoolManager::Pmpm()->Destroy(m_pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PtabdescPlainWithColNameFormat
//
//	@doc:
//		Generate a plain table descriptor, where the column names are generated
//		using a format string containing %d
//
//---------------------------------------------------------------------------


CTableDescriptor *
CTestUtils::PtabdescPlainWithColNameFormat
	(
	IMemoryPool *pmp,
	ULONG ulCols,
	IMDId *pmdid,
	const WCHAR *wszColNameFormat,
	const CName &nameTable,
	BOOL fNullable // define nullable columns
	)
{
	GPOS_ASSERT(0 < ulCols);

	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);
	CWStringDynamic *pstrName = GPOS_NEW(pmp) CWStringDynamic(pmp);
	CTableDescriptor *ptabdesc = GPOS_NEW(pmp) CTableDescriptor
											(
											pmp, 
											pmdid, 
											nameTable, 
											false, // fConvertHashToRandom
											IMDRelation::EreldistrRandom,
											IMDRelation::ErelstorageHeap, 
											0 // ulExecuteAsUser
											);

	for (ULONG i = 0; i < ulCols; i++)
	{
		pstrName->Reset();
		pstrName->AppendFormat(wszColNameFormat, i);

		// create a shallow constant string to embed in a name
		CWStringConst strName(pstrName->Wsz());
		CName nameColumnInt(&strName);

		CColumnDescriptor *pcoldescInt = GPOS_NEW(pmp) CColumnDescriptor(pmp, pmdtypeint4, nameColumnInt, i + 1, fNullable);
		ptabdesc->AddColumn(pcoldescInt);
	}

	GPOS_DELETE(pstrName);

	return ptabdesc;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PtabdescPlain
//
//	@doc:
//		Generate a plain table descriptor
//
//---------------------------------------------------------------------------
CTableDescriptor *
CTestUtils::PtabdescPlain
	(
	IMemoryPool *pmp,
	ULONG ulCols,
	IMDId *pmdid,
	const CName &nameTable,
	BOOL fNullable // define nullable columns
	)
{
	return PtabdescPlainWithColNameFormat(
			pmp, ulCols, pmdid, GPOS_WSZ_LIT("column_%04d"), nameTable, fNullable);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PtabdescCreate
//
//	@doc:
//		Generate a table descriptor
//
//---------------------------------------------------------------------------
CTableDescriptor *
CTestUtils::PtabdescCreate
	(
	IMemoryPool *pmp,
	ULONG ulCols,
	IMDId *pmdid,
	const CName &nameTable,
	BOOL fPartitioned
	)
{
	CTableDescriptor *ptabdesc = PtabdescPlain(pmp, ulCols, pmdid, nameTable);

	if (fPartitioned)
	{
		ptabdesc->AddPartitionColumn(0);
	}
	
	// create a keyset containing the first column
	CBitSet *pbs = GPOS_NEW(pmp) CBitSet(pmp, ulCols);
	pbs->FExchangeSet(0);
#ifdef GPOS_DEBUG
	BOOL fSuccess =
#endif // GPOS_DEBUG
		ptabdesc->FAddKeySet(pbs);
	GPOS_ASSERT(fSuccess);

	return ptabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGet
//
//	@doc:
//		Generate a get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGet
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	const CWStringConst *pstrTableAlias
	)
{
	GPOS_ASSERT(NULL != ptabdesc);

	return GPOS_NEW(pmp) CExpression
					(
					pmp,                   
					GPOS_NEW(pmp) CLogicalGet
								(
								pmp,
								GPOS_NEW(pmp) CName(pmp, CName(pstrTableAlias)),
								ptabdesc
								)
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGetNullable
//
//	@doc:
//		Generate a get expression over table with nullable columns
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGetNullable
	(
	IMemoryPool *pmp,
	OID oidTable,
	const CWStringConst *pstrTableName,
	const CWStringConst *pstrTableAlias
	)
{
	CWStringConst strName(pstrTableName->Wsz());
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(oidTable, 1, 1);
	CTableDescriptor *ptabdesc = CTestUtils::PtabdescPlain(pmp, 3, pmdid, CName(&strName), true /*fNullable*/);
	CWStringConst strAlias(pstrTableAlias->Wsz());

	return PexprLogicalGet(pmp, ptabdesc, &strAlias);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGet
//
//	@doc:
//		Generate a get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGet
	(
	IMemoryPool *pmp,
	CWStringConst *pstrTableName,
	CWStringConst *pstrTableAlias,
	ULONG ulTableId
	)
{
	CTableDescriptor *ptabdesc = PtabdescCreate
									(
									pmp,
									GPOPT_TEST_REL_WIDTH,
									GPOS_NEW(pmp) CMDIdGPDB(ulTableId, 1, 1),
									CName(pstrTableName)
									);

	CWStringConst strAlias(pstrTableAlias->Wsz());
	return PexprLogicalGet(pmp, ptabdesc, &strAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGet
//
//	@doc:
//		Generate a randomized get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGet
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("BaseTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 3, pmdid, CName(&strName));
	CWStringConst strAlias(GPOS_WSZ_LIT("BaseTableAlias"));

	return PexprLogicalGet(pmp, ptabdesc, &strAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalExternalGet
//
//	@doc:
//		Generate a randomized external get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalExternalGet
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("ExternalTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 3, pmdid, CName(&strName));
	CWStringConst strAlias(GPOS_WSZ_LIT("ExternalTableAlias"));

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalExternalGet
								(
								pmp,
								GPOS_NEW(pmp) CName(pmp, &strAlias),
								ptabdesc
								)
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGetPartitioned
//
//	@doc:
//		Generate a randomized get expression for a partitioned table
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGetPartitioned
	(
	IMemoryPool *pmp
	)
{
	ULONG ulAttributes = 2;
	CWStringConst strName(GPOS_WSZ_LIT("PartTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID_PARTITIONED);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, ulAttributes, pmdid, CName(&strName), true /*fPartitioned*/);
	CWStringConst strAlias(GPOS_WSZ_LIT("PartTableAlias"));

	return PexprLogicalGet(pmp, ptabdesc, &strAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalDynamicGetWithIndexes
//
//	@doc:
//		Generate a randomized get expression for a partitioned table
//		with indexes
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalDynamicGetWithIndexes
	(
	IMemoryPool *pmp
	)
{
	ULONG ulAttributes = 2;
	CWStringConst strName(GPOS_WSZ_LIT("P1"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID_PARTITIONED_WITH_INDEXES);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, ulAttributes, pmdid, CName(&strName), true /*fPartitioned*/);
	CWStringConst strAlias(GPOS_WSZ_LIT("P1Alias"));

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalDynamicGet
								(
								pmp,
								GPOS_NEW(pmp) CName(pmp, CName(&strAlias)),
								ptabdesc,
								0 // ulPartIndex
								)
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelect
//
//	@doc:
//		Generate a Select expression with a random equality predicate
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelect
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pexpr);

	// get any two columns
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft =  pcrs->PcrAny();
	CColRef *pcrRight = pcrs->PcrAny();
	CExpression *pexprPredicate = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);

	return CUtils::PexprLogicalSelect(pmp, pexpr, pexprPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelect
//
//	@doc:
//		Generate select expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelect
	(
	IMemoryPool *pmp,
	CWStringConst *pstrTableName,
	CWStringConst *pstrTableAlias,
	ULONG ulTableId
	)
{
	CExpression *pexprGet = PexprLogicalGet(pmp, pstrTableName, pstrTableAlias, ulTableId);
	return PexprLogicalSelect(pmp, pexprGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelect
//
//	@doc:
//		Generate randomized Select expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelect
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalSelect(pmp, PexprLogicalGet(pmp));
}

//---------------------------------------------------------------------------
//	@function:
// 		CTestUtils::PexprLogicalSelectWithContradiction
//
//	@doc:
//		Generate a select expression with a contradiction
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithContradiction
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexpr = PexprLogicalSelect(pmp, PexprLogicalGet(pmp));
	// get any column
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();

	CExpression *pexprConstFirst = CUtils::PexprScalarConstInt4(pmp, 3 /*iVal*/);
	CExpression *pexprPredFirst = CUtils::PexprScalarEqCmp(pmp, pcr, pexprConstFirst);

	CExpression *pexprConstSecond = CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/);
	CExpression *pexprPredSecond = CUtils::PexprScalarEqCmp(pmp, pcr, pexprConstSecond);

	CExpression *pexprPredicate = CPredicateUtils::PexprConjunction(pmp, pexprPredFirst, pexprPredSecond);
	pexprPredFirst->Release();
	pexprPredSecond->Release();

	return CUtils::PexprLogicalSelect(pmp, pexpr, pexprPredicate);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectPartitioned
//
//	@doc:
//		Generate a randomized select expression over a partitioned table
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectPartitioned
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprGet = PexprLogicalGetPartitioned(pmp);

	// extract first partition key
	CLogicalGet *popGet = CLogicalGet::PopConvert(pexprGet->Pop());
	const DrgDrgPcr *pdrgpdrgpcr = popGet->PdrgpdrgpcrPartColumns();

	GPOS_ASSERT(0 < pdrgpdrgpcr->UlSafeLength());
	DrgPcr *pdrgpcr = (*pdrgpdrgpcr)[0];
	GPOS_ASSERT(1 == pdrgpcr->UlLength());
	CColRef *pcrPartKey = (*pdrgpcr)[0];

	// construct a comparison pk = 5
	INT iVal = 5;
	CExpression *pexprScalar = CUtils::PexprScalarEqCmp
										(
										pmp,
										CUtils::PexprScalarIdent(pmp, pcrPartKey),
										CUtils::PexprScalarConstInt4(pmp, iVal)
										);

	return CUtils::PexprLogicalSelect
						(
						pmp,
						pexprGet,
						pexprScalar
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalAssert
//
//	@doc:
//		Generate randomized Assert expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalAssert
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprGet = PexprLogicalGet(pmp);
	
	// get any two columns
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft =  pcrs->PcrAny();
	CColRef *pcrRight = pcrs->PcrAny();
	CExpression *pexprPredicate = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);
	
	CWStringConst *pstrErrMsg = CXformUtils::PstrErrorMessage(pmp, gpos::CException::ExmaSQL, gpos::CException::ExmiSQLTest, GPOS_WSZ_LIT("Test msg"));
	CExpression *pexprAssertConstraint = GPOS_NEW(pmp) CExpression
											(
											pmp,
											GPOS_NEW(pmp) CScalarAssertConstraint(pmp, pstrErrMsg),
											pexprPredicate
											);
	CExpression *pexprAssertPredicate = GPOS_NEW(pmp) CExpression
												(
												pmp,
												GPOS_NEW(pmp) CScalarAssertConstraintList(pmp),
												pexprAssertConstraint
												);
		
	CLogicalAssert *popAssert = 
			GPOS_NEW(pmp) CLogicalAssert
						(
						pmp, 
						GPOS_NEW(pmp) CException(gpos::CException::ExmaSQL, gpos::CException::ExmiSQLTest)						
						);
	
	return GPOS_NEW(pmp) CExpression(pmp, popAssert, pexprGet, pexprAssertPredicate);
}

//---------------------------------------------------------------------------
//		CTestUtils::Pexpr3WayJoinPartitioned
//
//	@doc:
//		Generate a 3-way join including a partitioned table
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::Pexpr3WayJoinPartitioned
	(
	IMemoryPool *pmp
	)
{
	return
		PexprLogicalJoin<CLogicalInnerJoin>(pmp, PexprLogicalGet(pmp), PexprJoinPartitionedOuter<CLogicalInnerJoin>(pmp));

}

//---------------------------------------------------------------------------
//		CTestUtils::Pexpr4WayJoinPartitioned
//
//	@doc:
//		Generate a 4-way join including a partitioned table
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::Pexpr4WayJoinPartitioned
	(
	IMemoryPool *pmp
	)
{
	return
		PexprLogicalJoin<CLogicalInnerJoin>(pmp, PexprLogicalGet(pmp), Pexpr3WayJoinPartitioned(pmp));
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithNestedAnd
//
//	@doc:
//		Generate a random select expression with nested AND tree
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithNestedAnd
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprGet = PexprLogicalGet(pmp);
	CExpression *pexprPred = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopAnd);

	return CUtils::PexprLogicalSelect(pmp, pexprGet, pexprPred);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithNestedOr
//
//	@doc:
//		Generate a random select expression with nested OR tree
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithNestedOr
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprGet = PexprLogicalGet(pmp);
	CExpression *pexprPred = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopOr);

	return CUtils::PexprLogicalSelect(pmp, pexprGet, pexprPred);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithEvenNestedNot
//
//	@doc:
//		Generate a random select expression with an even number of
//		nested NOT nodes
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithEvenNestedNot
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprGet = PexprLogicalGet(pmp);
	CExpression *pexprPred = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopNot);

	return CUtils::PexprLogicalSelect(pmp, pexprGet, pexprPred);
} 


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprScIdentCmpScIdent
//
//	@doc:
//		Generate a scalar expression comparing scalar identifiers
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprScIdentCmpScIdent
	(
	IMemoryPool *pmp,
	CExpression *pexprLeft,
	CExpression *pexprRight,
	IMDType::ECmpType ecmpt
	)
{
	GPOS_ASSERT(NULL != pexprLeft);
	GPOS_ASSERT(NULL != pexprRight);
	GPOS_ASSERT(ecmpt <= IMDType::EcmptOther);

	CColRefSet *pcrsLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft =  pcrsLeft->PcrAny();

	CColRefSet *pcrsRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive())->PcrsOutput();
	CColRef *pcrRight =  pcrsRight->PcrAny();

	CExpression *pexprPred = CUtils::PexprScalarCmp(pmp, pcrLeft, pcrRight, ecmpt);

	return pexprPred;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprScIdentCmpConst
//
//	@doc:
//		Generate a scalar expression comparing scalar identifier to a constant
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprScIdentCmpConst
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	IMDType::ECmpType ecmpt,
	ULONG ulVal
	)
{
	GPOS_ASSERT(NULL != pexpr);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft =  pcrs->PcrAny();
	CExpression *pexprUl = CUtils::PexprScalarConstInt4(pmp, ulVal);

	CExpression *pexprPred = CUtils::PexprScalarCmp(pmp, pcrLeft, pexprUl, ecmpt);

	return pexprPred;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectCmpToConst
//
//	@doc:
//		Generate a Select expression with an equality predicate on the first
//		column and a constant
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectCmpToConst
	(
	IMemoryPool *pmp
	)
{
	// generate a get expression
	CExpression *pexpr = PexprLogicalGet(pmp);
	CExpression *pexprPred = PexprScIdentCmpConst(pmp, pexpr, IMDType::EcmptEq /* ecmpt */, 10 /* ulVal */);

	return CUtils::PexprLogicalSelect(pmp, pexpr, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectArrayCmp
//
//	@doc:
//		Generate a Select expression with an array compare
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectArrayCmp
	(
	IMemoryPool *pmp
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a get expression
	CExpression *pexprGet = PexprLogicalGet(pmp);

	// get the first column
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	CColRef *pcr =  pcrs->PcrAny();
	CExpression *pexprIdent = CUtils::PexprScalarIdent(pmp, pcr);
	
	// construct an array of integers
	DrgPexpr *pdrgpexprArrayElems = GPOS_NEW(pmp) DrgPexpr(pmp);
	
	for (ULONG ul = 0; ul < 5; ul++)
	{
		CExpression *pexprArrayElem = CUtils::PexprScalarConstInt4(pmp, ul);
		pdrgpexprArrayElems->Append(pexprArrayElem);
	}

	// get column type mdid and mdid of the array type corresponding to that type 
	IMDId *pmdidColType = pcr->Pmdtype()->Pmdid();
	IMDId *pmdidArrType = pcr->Pmdtype()->PmdidTypeArray();
	IMDId *pmdidCmpOp = pcr->Pmdtype()->PmdidCmp(IMDType::EcmptEq);

	pmdidColType->AddRef();
	pmdidArrType->AddRef();
	pmdidCmpOp ->AddRef();
	
	const CMDName mdname = pmda->Pmdscop(pmdidCmpOp)->Mdname();
	CWStringConst strOp(mdname.Pstr()->Wsz());
		
	CExpression *pexprArray = GPOS_NEW(pmp) CExpression
										(
										pmp,
										GPOS_NEW(pmp) CScalarArray(pmp, pmdidColType, pmdidArrType, false /*fMultiDimensional*/),
										pdrgpexprArrayElems
										);
	
	CExpression *pexprArrayCmp = 
			GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CScalarArrayCmp(pmp, pmdidCmpOp, GPOS_NEW(pmp) CWStringConst(pmp, strOp.Wsz()), CScalarArrayCmp::EarrcmpAny),
						pexprIdent,
						pexprArray
						);
 
	return CUtils::PexprLogicalSelect(pmp, pexprGet, pexprArrayCmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithOddNestedNot
//
//	@doc:
//		Generate a random select expression with an odd number of
//		nested NOT nodes
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithOddNestedNot
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprGet = PexprLogicalGet(pmp);
	CExpression *pexprPred = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopNot);
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprPred);
	CExpression *pexprNot =  CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopNot, pdrgpexpr);

	return CUtils::PexprLogicalSelect(pmp, pexprGet, pexprNot);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithNestedAndOrNot
//
//	@doc:
//		Generate a random select expression with nested AND-OR-NOT predicate
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithNestedAndOrNot
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL != pmp);

	CExpression *pexprGet = PexprLogicalGet(pmp);
	CExpression *pexprPredAnd = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopAnd);
	CExpression *pexprPredOr = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopOr);
	CExpression *pexprPredNot = PexprScalarNestedPreds(pmp, pexprGet, CScalarBoolOp::EboolopNot);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprPredAnd);
	pdrgpexpr->Append(pexprPredOr);
	pdrgpexpr->Append(pexprPredNot);
	CExpression *pexprOr =  CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);
	CExpression *pexprPred =  CUtils::PexprNegate(pmp, pexprOr);

	return CUtils::PexprLogicalSelect(pmp, pexprGet, pexprPred);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSubqueryWithConstTableGet
//
//	@doc:
//		Generate Select expression with Any subquery predicate over a const
//		table get
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSubqueryWithConstTableGet
	(
	IMemoryPool *pmp,
	COperator::EOperatorId eopid
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(COperator::EopScalarSubqueryAny == eopid ||
				COperator::EopScalarSubqueryAll == eopid);

	CWStringConst strNameR(GPOS_WSZ_LIT("Rel1"));
	CMDIdGPDB *pmdidR = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_TEST_REL_OID1, 1, 1);
	CTableDescriptor *ptabdescR = PtabdescCreate(pmp, 3 /*ulCols*/, pmdidR, CName(&strNameR));

	CExpression *pexprOuter = PexprLogicalGet(pmp, ptabdescR, &strNameR);
	CExpression *pexprConstTableGet = PexprConstTableGet(pmp, 3 /* ulElements */);

	// get random columns from inner expression
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprConstTableGet->PdpDerive())->PcrsOutput();
	const CColRef *pcrInner = pcrs->PcrAny();

	// get random columns from outer expression
	pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	const CColRef *pcrOuter = pcrs->PcrAny();

	const CWStringConst *pstr = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("="));

	CExpression *pexprSubquery = NULL;
	if (COperator::EopScalarSubqueryAny == eopid)
	{
		// construct ANY subquery expression
		pexprSubquery = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarSubqueryAny(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
									pexprConstTableGet,
									CUtils::PexprScalarIdent(pmp, pcrOuter)
									);
	}
	else
	{
		// construct ALL subquery expression
		pexprSubquery = GPOS_NEW(pmp) CExpression
									(
									pmp,
									GPOS_NEW(pmp) CScalarSubqueryAll(pmp, GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP), pstr, pcrInner),
									pexprConstTableGet,
									CUtils::PexprScalarIdent(pmp, pcrOuter)
									);

	}

	return CUtils::PexprLogicalSelect(pmp, pexprOuter, pexprSubquery);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithConstAnySubquery
//
//	@doc:
//		Generate a random select expression with constant ANY subquery
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithConstAnySubquery
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalSubqueryWithConstTableGet(pmp, COperator::EopScalarSubqueryAny);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithConstAllSubquery
//
//	@doc:
//		Generate a random select expression with constant ALL subquery
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithConstAllSubquery
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalSubqueryWithConstTableGet(pmp, COperator::EopScalarSubqueryAll);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectCorrelated
//
//	@doc:
//		Generate correlated select; wrapper
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectCorrelated
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprOuter = PexprLogicalGet(pmp);
	CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();

	CExpression *pexpr = PexprLogicalSelectCorrelated(pmp, pcrsOuter, 8);

	pexprOuter->Release();

	return pexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectOnOuterJoin
//
//	@doc:
//		Generate a select on top of outer join expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectOnOuterJoin
	(
	IMemoryPool *pmp
	)
{
	// generate a pair of get expressions
	CExpression *pexprOuter = NULL;
	CExpression *pexprInner = NULL;
	CSubqueryTestUtils::GenerateGetExpressions(pmp, &pexprOuter, &pexprInner);

	const CColRef *pcrOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput()->PcrAny();
	const CColRef *pcrInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprOuterJoinPred = CUtils::PexprScalarEqCmp(pmp, pcrOuter, pcrInner);
	CExpression *pexprOuterJoin =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp),
				pexprOuter,
				pexprInner,
				pexprOuterJoinPred
				);
	CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrInner, CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/));

	return CUtils::PexprLogicalSelect(pmp, pexprOuterJoin, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectCorrelated
//
//	@doc:
//		Generate correlated select, different predicates depending on nesting
//		level: correlated, exclusively external, non-correlated
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectCorrelated
	(
	IMemoryPool *pmp,
	CColRefSet *pcrsOuter,
	ULONG ulLevel
	)
{
	GPOS_CHECK_STACK_SIZE;

	if (0 == ulLevel)
	{
		return PexprLogicalGet(pmp);
	}

	CExpression *pexpr = PexprLogicalSelectCorrelated(pmp, pcrsOuter, ulLevel - 1);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	GPOS_ASSERT(pcrsOuter->FDisjoint(pcrs));

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	switch (ulLevel % 6)
	{
		case 4:
			// outer only
			EqualityPredicate(pmp, pcrsOuter, pcrsOuter, pdrgpexpr);

			// inner only
			EqualityPredicate(pmp, pcrs, pcrs, pdrgpexpr);

			// regular correlation
			EqualityPredicate(pmp, pcrs, pcrsOuter, pdrgpexpr);
			break;

		case 3:
			// outer only
			EqualityPredicate(pmp, pcrsOuter, pcrsOuter, pdrgpexpr);
			break;

		case 2:
			// inner only
			EqualityPredicate(pmp, pcrs, pcrs, pdrgpexpr);

			// regular correlation
			EqualityPredicate(pmp, pcrs, pcrsOuter, pdrgpexpr);
			break;

		case 1:
			// inner only
			EqualityPredicate(pmp, pcrs, pcrs, pdrgpexpr);
			break;

		case 0:
			// regular correlation
			EqualityPredicate(pmp, pcrs, pcrsOuter, pdrgpexpr);
			break;
	}

	CExpression *pexprCorrelation = CPredicateUtils::PexprConjunction(pmp, pdrgpexpr);

	// assemble select
	CExpression *pexprResult =
			GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalSelect(pmp),
					pexpr,
					pexprCorrelation);

	return pexprResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalProject
//
//	@doc:
//		Generate a Project expression that re-maps a single column
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalProject
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CColRef *pcr,
	CColRef *pcrNew
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(NULL != pcr);
	GPOS_ASSERT(NULL != pcrNew);

	return GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalProject(pmp),
			pexpr,
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CScalarProjectList(pmp),
				GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarProjectElement(pmp, pcrNew),
					GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CScalarIdent(pmp, pcr)
						)
					)
				)
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalProject
//
//	@doc:
//		Generate a randomized project expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalProject
	(
	IMemoryPool *pmp
	)
{
	// generate a get expression
	CExpression *pexpr = PexprLogicalGet(pmp);

	// get output columns
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();

	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();
	CColRef *pcr =  pcrs->PcrAny();

	// create new column to which we will an existing one remap to
	CColRef *pcrNew = pcf->PcrCreate(pcr);

	return PexprLogicalProject(pmp, pexpr, pcr, pcrNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalProjectGbAggCorrelated
//
//	@doc:
//		Generate correlated Project over GbAgg
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalProjectGbAggCorrelated
	(
	IMemoryPool *pmp
	)
{
	// create a GbAgg expression
	CExpression *pexprGbAgg = PexprLogicalGbAggCorrelated(pmp);

	CExpression *pexprPrjList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp));
	CExpression *pexprProject =
		GPOS_NEW(pmp) CExpression(pmp,
							 GPOS_NEW(pmp) CLogicalProject(pmp),
							 pexprGbAgg,
							 pexprPrjList);

	return pexprProject;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalJoinCorrelated
//
//	@doc:
//		Generate correlated join
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalJoinCorrelated
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprLeft = PexprLogicalSelectCorrelated(pmp);
	CExpression *pexprRight = PexprLogicalSelectCorrelated(pmp);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprLeft);
	pdrgpexpr->Append(pexprRight);

	CColRefSet *pcrsLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive())->PcrsOutput();

	EqualityPredicate(pmp, pcrsLeft, pcrsRight, pdrgpexpr);

	return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalInnerJoin(pmp), pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalJoinCorrelated
//
//	@doc:
//		Generate join with a partitioned and indexed inner child
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalJoinWithPartitionedAndIndexedInnerChild
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprLeft = PexprLogicalGet(pmp);
	CExpression *pexprRight = PexprLogicalDynamicGetWithIndexes(pmp);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprLeft);
	pdrgpexpr->Append(pexprRight);

	CColRefSet *pcrsLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive())->PcrsOutput();
	CColRefSet *pcrsRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive())->PcrsOutput();

	EqualityPredicate(pmp, pcrsLeft, pcrsRight, pdrgpexpr);

	return GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CLogicalInnerJoin(pmp), pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalNAryJoin
//
//	@doc:
//		Generate randomized n-ary join expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalNAryJoin
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);
	GPOS_ASSERT(2 < pdrgpexpr->UlLength());

	return GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CLogicalNAryJoin(pmp),
			pdrgpexpr
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalNAryJoin
//
//	@doc:
//		Generate randomized n-ary join expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalNAryJoin
	(
	IMemoryPool *pmp
	)
{
	const ULONG  ulRels = 4;

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	DrgPexpr *pdrgpexprConjuncts = GPOS_NEW(pmp) DrgPexpr(pmp);

	CExpression *pexprInit = PexprLogicalGet(pmp);
	pdrgpexpr->Append(pexprInit);

	// build pairwise joins, extract predicate and input, then discard join
	for (ULONG ul = 0; ul < ulRels; ul++)
	{
		CExpression *pexprNext = PexprLogicalGet(pmp);
		pdrgpexpr->Append(pexprNext);

		pexprInit->AddRef();
		pexprNext->AddRef();

		CExpression *pexprJoin = PexprLogicalJoin<CLogicalInnerJoin>(pmp, pexprInit, pexprNext);

		CExpression *pexprPredicate = (*pexprJoin)[2];

		pexprPredicate->AddRef();
		pdrgpexprConjuncts->Append(pexprPredicate);

		pexprJoin->Release();
	}

	// add predicate built of conjuncts to the input array
	pdrgpexpr->Append(CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexprConjuncts));

	return PexprLogicalNAryJoin(pmp, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLeftOuterJoinOnNAryJoin
//
//	@doc:
//		Generate left outer join on top of n-ary join expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLeftOuterJoinOnNAryJoin
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprNAryJoin = PexprLogicalNAryJoin(pmp);
	CExpression *pexprLOJInnerChild = PexprLogicalGet(pmp);

	// get a random column from LOJ inner child output
	CColRef *pcrLeft1 = CDrvdPropRelational::Pdprel(pexprLOJInnerChild->PdpDerive())->PcrsOutput()->PcrAny();

	// get a random column from NAry join second child
	CColRef *pcrLeft2 = CDrvdPropRelational::Pdprel((*pexprNAryJoin)[1]->PdpDerive())->PcrsOutput()->PcrAny();

	// get a random column from NAry join output
	CColRef *pcrRight = CDrvdPropRelational::Pdprel(pexprNAryJoin->PdpDerive())->PcrsOutput()->PcrAny();

	CExpression *pexprPred1 = CUtils::PexprScalarEqCmp(pmp, pcrLeft1, pcrRight);
	CExpression *pexprPred2 = CUtils::PexprScalarEqCmp(pmp, pcrLeft2, pcrRight);
	CExpression *pexprPred = CPredicateUtils::PexprConjunction(pmp, pexprPred1, pexprPred2);
	pexprPred1->Release();
	pexprPred2->Release();

	return GPOS_NEW(pmp) CExpression (pmp, GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp), pexprNAryJoin, pexprLOJInnerChild, pexprPred);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprNAryJoinOnLeftOuterJoin
//
//	@doc:
//		Generate n-ary join expression on top of left outer join
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprNAryJoinOnLeftOuterJoin
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprNAryJoin = PexprLogicalNAryJoin(pmp);
	CColRef *pcrNAryJoin = CDrvdPropRelational::Pdprel((*pexprNAryJoin)[0]->PdpDerive())->PcrsOutput()->PcrAny();
	CExpression *pexprScalar = (*pexprNAryJoin)[pexprNAryJoin->UlArity() - 1];

	// copy NAry-Join children
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	CUtils::AddRefAppend<CExpression>(pdrgpexpr, pexprNAryJoin->PdrgPexpr());

	// generate LOJ expression
	CExpression *pexprLOJOuterChild = PexprLogicalGet(pmp);
	CExpression *pexprLOJInnerChild = PexprLogicalGet(pmp);

	// get a random column from LOJ outer child output
	CColRef *pcrLeft = CDrvdPropRelational::Pdprel(pexprLOJOuterChild->PdpDerive())->PcrsOutput()->PcrAny();

	// get a random column from LOJ inner child output
	CColRef *pcrRight = CDrvdPropRelational::Pdprel(pexprLOJInnerChild->PdpDerive())->PcrsOutput()->PcrAny();

	CExpression *pexprPred = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);
	CExpression *pexprLOJ = GPOS_NEW(pmp) CExpression (pmp, GPOS_NEW(pmp) CLogicalLeftOuterJoin(pmp), pexprLOJOuterChild, pexprLOJInnerChild, pexprPred);

	// replace NAry-Join scalar predicate with LOJ expression
	pdrgpexpr->Replace(pdrgpexpr->UlLength() - 1, pexprLOJ);

	// create new scalar predicate for NAry join
	CExpression *pexprNewPred = CUtils::PexprScalarEqCmp(pmp, pcrNAryJoin, pcrRight);
	pdrgpexpr->Append(CPredicateUtils::PexprConjunction(pmp, pexprScalar, pexprNewPred));
	pexprNAryJoin->Release();
	pexprNewPred->Release();

	return GPOS_NEW(pmp) CExpression (pmp, GPOS_NEW(pmp) CLogicalNAryJoin(pmp), pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalLimit
//
//	@doc:
//		Generate a limit expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalLimit
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	LINT iStart,
	LINT iRows,
	BOOL fGlobal,
	BOOL fHasCount
	)
{
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(0 <= iStart);
	GPOS_ASSERT(0 <= iRows);

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalLimit(pmp, pos, fGlobal, fHasCount, false /*fTopLimitUnderDML*/),
					pexpr,
					CUtils::PexprScalarConstInt8(pmp, iStart),
					CUtils::PexprScalarConstInt8(pmp, iRows)
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalLimit
//
//	@doc:
//		Generate a randomized limit expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalLimit
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalLimit
				(
				pmp,
				PexprLogicalGet(pmp),
				0, // iStart
				100, // iRows
				true, // fGlobal
				true // fHasCount
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGbAggWithSum
//
//	@doc:
//		Generate a randomized aggregate expression with sum agg function
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGbAggWithSum
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprGb = PexprLogicalGbAgg(pmp);

	// get a random column from Gb child
	CColRef *pcr =
		CDrvdPropRelational::Pdprel((*pexprGb)[0]->PdpDerive())->PcrsOutput()->PcrAny();

	// generate a SUM expression
	CExpression *pexprProjElem = PexprPrjElemWithSum(pmp, pcr);
	CExpression *pexprPrjList =
		GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp), pexprProjElem);

	(*pexprGb)[0]->AddRef();
	DrgPcr *pdrgpcr = CLogicalGbAgg::PopConvert(pexprGb->Pop())->Pdrgpcr();
	pdrgpcr->AddRef();
	CExpression *pexprGbWithSum = CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr, (*pexprGb)[0], pexprPrjList);

	// release old Gb expression
	pexprGb->Release();

	return pexprGbWithSum;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGbAggWithInput
//
//	@doc:
//		Generate a randomized aggregate expression
//
//---------------------------------------------------------------------------
CExpression *CTestUtils::PexprLogicalGbAggWithInput
	(
	IMemoryPool *pmp,
	CExpression *pexprInput
	)
{
	// get the first few columns
	CDrvdPropRelational *pdprel = CDrvdPropRelational::Pdprel(pexprInput->PdpDerive());
	CColRefSet *pcrs = pdprel->PcrsOutput();

	ULONG ulCols = std::min(3u, pcrs->CElements());

	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	CColRefSetIter crsi(*pcrs);
	for (ULONG i = 0; i < ulCols; i++)
	{
		(void) crsi.FAdvance();
		pdrgpcr->Append(crsi.Pcr());
	}

	CExpression *pexprList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp));
	return CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr, pexprInput, pexprList);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGbAgg
//
//	@doc:
//		Generate a randomized aggregate expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGbAgg
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalGbAggWithInput(pmp, PexprLogicalGet(pmp));
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprPrjElemWithSum
//
//	@doc:
//		Generate a project element with sum agg function
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprPrjElemWithSum
	(
	IMemoryPool *pmp,
	CColRef *pcr
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	// generate a SUM expression
	CMDIdGPDB *pmdidSumAgg = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_SUM_AGG);
	CWStringConst *pstrAggFunc = GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("sum"));
	CExpression *pexprScalarAgg = CUtils::PexprAggFunc
									(
									pmp,
									pmdidSumAgg,
									pstrAggFunc,
									pcr,
									false /*fDistinct*/,
									EaggfuncstageGlobal /*eaggfuncstage*/,
									false /*fSplit*/
									);

	// map a computed column to SUM expression
	IMDId *pmdidType = CScalar::PopConvert(pexprScalarAgg->Pop())->PmdidType();
	const IMDType *pmdtype = pmda->Pmdtype(pmdidType);
	CWStringConst str(GPOS_WSZ_LIT("sum_col"));
	CName name(pmp, &str);
	CColRef *pcrComputed = COptCtxt::PoctxtFromTLS()->Pcf()->PcrCreate(pmdtype, name);

	return CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprScalarAgg);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGbAggOverJoin
//
//	@doc:
//		Generate a randomized group by over join expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGbAggOverJoin
	(
	IMemoryPool *pmp
	)
{
	// generate a join expression
	CExpression *pexprJoin = PexprLogicalJoin<CLogicalInnerJoin>(pmp);

	// include one grouping column
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	CColRef *pcr = CDrvdPropRelational::Pdprel(pexprJoin->PdpDerive())->PcrsOutput()->PcrAny();
	pdrgpcr->Append(pcr);

	return CUtils::PexprLogicalGbAggGlobal
				(
				pmp,
				pdrgpcr,
				pexprJoin,
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp))
				);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGbAggDedupOverInnerJoin
//
//	@doc:
//		Generate a dedup group by over inner join expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGbAggDedupOverInnerJoin
	(
	IMemoryPool *pmp
	)
{
	// generate a join expression
	CExpression *pexprJoin = PexprLogicalJoin<CLogicalInnerJoin>(pmp);

	// get join's outer child
	CExpression *pexprOuter = (*pexprJoin)[0];
	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();

	// grouping columns: all columns from outer child
	DrgPcr *pdrgpcrGrp = pcrs->Pdrgpcr(pmp);

	// outer child keys: get a random column from its output columns
	CColRef *pcr = pcrs->PcrAny();
	DrgPcr *pdrgpcrKeys = GPOS_NEW(pmp) DrgPcr(pmp);
	pdrgpcrKeys->Append(pcr);

	return GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CLogicalGbAggDeduplicate(pmp, pdrgpcrGrp, COperator::EgbaggtypeGlobal  /*egbaggtype*/, pdrgpcrKeys),
				pexprJoin,
				GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp))
				);
	}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalGbAggCorrelated
//
//	@doc:
//		Generate correlated GbAgg
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalGbAggCorrelated
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexpr = PexprLogicalSelectCorrelated(pmp);

	// include one grouping column
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	CColRef *pcr = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput()->PcrAny();
	pdrgpcr->Append(pcr);

	CExpression *pexprList = GPOS_NEW(pmp) CExpression(pmp, GPOS_NEW(pmp) CScalarProjectList(pmp));
	return CUtils::PexprLogicalGbAggGlobal(pmp, pdrgpcr, pexpr, pexprList);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprConstTableGet
//
//	@doc:
//		Generate logical const table get expression with one column and 
//		the given number of elements
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprConstTableGet
	(
	IMemoryPool *pmp, 
	ULONG ulElements
	)
{
	GPOS_ASSERT(0 < ulElements);
	
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	const IMDTypeInt4 *pmdtypeint4 = pmda->PtMDType<IMDTypeInt4>(CTestUtils::m_sysidDefault);

	// create an integer column descriptor
	CWStringConst strName(GPOS_WSZ_LIT("A"));
	CName name(&strName);
	CColumnDescriptor *pcoldescInt = GPOS_NEW(pmp) CColumnDescriptor(pmp, pmdtypeint4, name, 1 /* iAttno */, false /*FNullable*/);
	
	DrgPcoldesc *pdrgpcoldesc = GPOS_NEW(pmp) DrgPcoldesc(pmp);
	pdrgpcoldesc->Append(pcoldescInt);

	// generate values
	DrgPdrgPdatum *pdrgpdrgpdatum = GPOS_NEW(pmp) DrgPdrgPdatum(pmp);
	
	for (ULONG ul = 0; ul < ulElements; ul++)
	{
		IDatumInt4 *pdatum = pmdtypeint4->PdatumInt4(pmp, ul, false /* fNull */);
		DrgPdatum *pdrgpdatum = GPOS_NEW(pmp) DrgPdatum(pmp);
		pdrgpdatum->Append((IDatum *) pdatum);
		pdrgpdrgpdatum->Append(pdrgpdatum);
	}
	CLogicalConstTableGet *popConstTableGet = GPOS_NEW(pmp) CLogicalConstTableGet(pmp, pdrgpcoldesc, pdrgpdrgpdatum);
	
	return GPOS_NEW(pmp) CExpression(pmp, popConstTableGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprConstTableGet5
//
//	@doc:
//		Generate logical const table get expression with one column and 5 elements
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprConstTableGet5
	(
	IMemoryPool *pmp
	)
{
	return PexprConstTableGet(pmp, 5 /* ulElements */);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalInsert
//
//	@doc:
//		Generate logical insert expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalInsert
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprCTG = PexprConstTableGet(pmp, 1 /* ulElements */);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprCTG->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcr = pcrs->Pdrgpcr(pmp);

	CWStringConst strName(GPOS_WSZ_LIT("BaseTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 1, pmdid, CName(&strName));

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalInsert(pmp, ptabdesc, pdrgpcr),
					pexprCTG
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalDelete
//
//	@doc:
//		Generate logical delete expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalDelete
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("BaseTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 1, pmdid, CName(&strName));
	CWStringConst strAlias(GPOS_WSZ_LIT("BaseTableAlias"));

	CExpression *pexprGet = PexprLogicalGet(pmp, ptabdesc, &strAlias);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcr = pcrs->Pdrgpcr(pmp);

	CColRef *pcr = pcrs->PcrFirst();

	ptabdesc->AddRef();

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalDelete(pmp, ptabdesc, pdrgpcr, pcr, pcr),
					pexprGet
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalUpdate
//
//	@doc:
//		Generate logical update expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalUpdate
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("BaseTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 1, pmdid, CName(&strName));
	CWStringConst strAlias(GPOS_WSZ_LIT("BaseTableAlias"));

	CExpression *pexprGet = PexprLogicalGet(pmp, ptabdesc, &strAlias);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexprGet->PdpDerive())->PcrsOutput();
	DrgPcr *pdrgpcrDelete = pcrs->Pdrgpcr(pmp);
	DrgPcr *pdrgpcrInsert = pcrs->Pdrgpcr(pmp);

	CColRef *pcr = pcrs->PcrFirst();

	ptabdesc->AddRef();

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalUpdate(pmp, ptabdesc, pdrgpcrDelete, pdrgpcrInsert, pcr, pcr, NULL /*pcrTupleOid*/),
					pexprGet
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalDynamicGet
//
//	@doc:
//		Generate a dynamic get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalDynamicGet
	(
	IMemoryPool *pmp,
	CTableDescriptor *ptabdesc,
	const CWStringConst *pstrTableAlias,
	ULONG ulPartIndex
	)
{
	GPOS_ASSERT(NULL != ptabdesc);

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalDynamicGet
								(
								pmp,
								GPOS_NEW(pmp) CName(pmp, CName(pstrTableAlias)),
								ptabdesc,
								ulPartIndex
								)
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalDynamicGet
//
//	@doc:
//		Generate a dynamic get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalDynamicGet
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("PartTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID_PARTITIONED, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 3, pmdid, CName(&strName), true /*fPartitioned*/);
	CWStringConst strAlias(GPOS_WSZ_LIT("PartTableAlias"));

	return PexprLogicalDynamicGet(pmp, ptabdesc, &strAlias, GPOPT_TEST_PART_INDEX);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithEqPredicateOverDynamicGet
//
//	@doc:
//		Generate a select over dynamic get expression with a predicate on the
//		partition key 
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithEqPredicateOverDynamicGet
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("PartTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID_PARTITIONED, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 3, pmdid, CName(&strName), true /*fPartitioned*/);
	CWStringConst strAlias(GPOS_WSZ_LIT("PartTableAlias"));

	CExpression *pexprDynamicGet = PexprLogicalDynamicGet(pmp, ptabdesc, &strAlias, GPOPT_TEST_PART_INDEX);
	
	// construct scalar comparison
	CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(pexprDynamicGet->Pop());
	DrgDrgPcr *pdrgpdrgpcr = popDynamicGet->PdrgpdrgpcrPart();
	CColRef *pcr = CUtils::PcrExtractPartKey(pdrgpdrgpcr, 0 /*ulLevel*/);
	CExpression *pexprScalarIdent = CUtils::PexprScalarIdent(pmp, pcr);
	
	CExpression *pexprConst = CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/);
	CExpression *pexprScalar = CUtils::PexprScalarEqCmp(pmp, pexprScalarIdent, pexprConst);
	
	return CUtils::PexprLogicalSelect(pmp, pexprDynamicGet, pexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSelectWithLTPredicateOverDynamicGet
//
//	@doc:
//		Generate a select over dynamic get expression with a predicate on the
//		partition key 
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSelectWithLTPredicateOverDynamicGet
	(
	IMemoryPool *pmp
	)
{
	CWStringConst strName(GPOS_WSZ_LIT("PartTable"));
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID_PARTITIONED, 1, 1);
	CTableDescriptor *ptabdesc = PtabdescCreate(pmp, 3, pmdid, CName(&strName), true /*fPartitioned*/);
	CWStringConst strAlias(GPOS_WSZ_LIT("PartTableAlias"));

	CExpression *pexprDynamicGet = PexprLogicalDynamicGet(pmp, ptabdesc, &strAlias, GPOPT_TEST_PART_INDEX);
	
	// construct scalar comparison
	CLogicalDynamicGet *popDynamicGet = CLogicalDynamicGet::PopConvert(pexprDynamicGet->Pop());
	DrgDrgPcr *pdrgpdrgpcr = popDynamicGet->PdrgpdrgpcrPart();
	CColRef *pcr = CUtils::PcrExtractPartKey(pdrgpdrgpcr, 0 /*ulLevel*/);
	CExpression *pexprScalarIdent = CUtils::PexprScalarIdent(pmp, pcr);
	
	CExpression *pexprConst = CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/);
	CExpression *pexprScalar = CUtils::PexprScalarCmp(pmp, pexprScalarIdent, pexprConst, IMDType::EcmptL);
	
	return CUtils::PexprLogicalSelect(pmp, pexprDynamicGet, pexprScalar);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalTVFTwoArgs
//
//	@doc:
//		Generate a logical TVF expression with 2 constant arguments
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalTVFTwoArgs
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalTVF(pmp, 2);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalTVFThreeArgs
//
//	@doc:
//		Generate a logical TVF expression with 3 constant arguments
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalTVFThreeArgs
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalTVF(pmp, 3);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalTVFNoArgs
//
//	@doc:
//		Generate a logical TVF expression with no arguments
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalTVFNoArgs
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalTVF(pmp, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalTVF
//
//	@doc:
//		Generate a TVF expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalTVF
	(
	IMemoryPool *pmp,
	ULONG ulArgs
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();

	const WCHAR *wszFuncName = GPOS_WSZ_LIT("generate_series");

	IMDId *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_GENERATE_SERIES);
	CWStringConst *pstrFuncName = GPOS_NEW(pmp) CWStringConst(pmp, wszFuncName);

	const IMDTypeInt8 *pmdtypeint8 = pmda->PtMDType<IMDTypeInt8>(CTestUtils::m_sysidDefault);

	// create an integer column descriptor
	CWStringConst strName(GPOS_WSZ_LIT("generate_series"));
	CName name(&strName);

	CColumnDescriptor *pcoldescInt = GPOS_NEW(pmp) CColumnDescriptor(pmp, pmdtypeint8, name, 1 /* iAttno */, false /*FNullable*/);

	DrgPcoldesc *pdrgpcoldesc = GPOS_NEW(pmp) DrgPcoldesc(pmp);
	pdrgpcoldesc->Append(pcoldescInt);

	IMDId *pmdidRetType = pmdtypeint8->Pmdid();
	pmdidRetType->AddRef();

	CLogicalTVF *popTVF = GPOS_NEW(pmp) CLogicalTVF
										(
										pmp,
										pmdid,
										pmdidRetType,
										pstrFuncName,
										pdrgpcoldesc
										);

	if (0 == ulArgs)
	{
		return GPOS_NEW(pmp) CExpression(pmp, popTVF);
	}

	DrgPexpr *pdrgpexprArgs = GPOS_NEW(pmp) DrgPexpr(pmp);

	for (ULONG ul = 0; ul < ulArgs; ul++)
	{
		ULONG ulArg = 1;
		IDatum *pdatum = (IDatum *) pmdtypeint8->PdatumInt8(pmp, ulArg, false /* fNull */);
		CScalarConst *popConst = GPOS_NEW(pmp) CScalarConst(pmp, pdatum);
		pdrgpexprArgs->Append(GPOS_NEW(pmp) CExpression(pmp, popConst));
	}

	return GPOS_NEW(pmp) CExpression(pmp, popTVF, pdrgpexprArgs);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalCTEProducerOverSelect
//
//	@doc:
//		Generate a CTE producer on top of a select
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalCTEProducerOverSelect
	(
	IMemoryPool *pmp,
	ULONG ulCTEId
	)
{
	CExpression *pexprSelect = CTestUtils::PexprLogicalSelect(pmp);
	DrgPcr *pdrgpcr = CDrvdPropRelational::Pdprel(pexprSelect->PdpDerive())->PcrsOutput()->Pdrgpcr(pmp);

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalCTEProducer(pmp, ulCTEId, pdrgpcr),
					pexprSelect
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalCTEProducerOverSelect
//
//	@doc:
//		Generate a CTE producer on top of a select
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalCTEProducerOverSelect
	(
	IMemoryPool *pmp
	)
{
	return PexprLogicalCTEProducerOverSelect(pmp, 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprCTETree
//
//	@doc:
//		Generate an expression with CTE producer and consumer
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprCTETree
	(
	IMemoryPool *pmp
	)
{
	ULONG ulCTEId = 0;
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	CExpression *pexprProducer = PexprLogicalCTEProducerOverSelect(pmp, ulCTEId);
	COptCtxt::PoctxtFromTLS()->Pcteinfo()->AddCTEProducer(pexprProducer);

	pdrgpexpr->Append(pexprProducer);

	DrgPcr *pdrgpcrProducer = CLogicalCTEProducer::PopConvert(pexprProducer->Pop())->Pdrgpcr();
	DrgPcr *pdrgpcrConsumer = CUtils::PdrgpcrCopy(pmp, pdrgpcrProducer);

	CExpression *pexprConsumer =
			GPOS_NEW(pmp) CExpression
						(
						pmp,
						GPOS_NEW(pmp) CLogicalCTEConsumer(pmp, ulCTEId, pdrgpcrConsumer)
						);

	pdrgpexpr->Append(pexprConsumer);

	return PexprLogicalSequence(pmp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSequence
//
//	@doc:
//		Generate a dynamic get expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSequence
	(
	IMemoryPool *pmp,
	DrgPexpr *pdrgpexpr
	)
{
	GPOS_ASSERT(NULL != pdrgpexpr);

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CLogicalSequence(pmp),
					pdrgpexpr
					);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSequence
//
//	@doc:
//		Generate a logical sequence expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSequence
	(
	IMemoryPool *pmp
	)
{
	const ULONG  ulRels = 2;
	const ULONG  ulCTGElements = 2;

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);

	// build an array of get expressions
	for (ULONG ul = 0; ul < ulRels; ul++)
	{
		CExpression *pexpr = PexprLogicalGet(pmp);
		pdrgpexpr->Append(pexpr);
	}

	CExpression *pexprCTG = PexprConstTableGet(pmp, ulCTGElements);
	pdrgpexpr->Append(pexprCTG);

	CExpression *pexprDynamicGet = PexprLogicalDynamicGet(pmp);
	pdrgpexpr->Append(pexprDynamicGet);

	return PexprLogicalSequence(pmp, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalUnion
//
//	@doc:
//		Generate tree of unions recursively
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalUnion
	(
	IMemoryPool *pmp,
	ULONG ulDepth
	)
{
	// stack check in recursion
	GPOS_CHECK_STACK_SIZE;

	CExpression *pexpr = NULL;

	if (0 == ulDepth)
	{
		// terminal case, generate table
		pexpr = PexprLogicalGet(pmp);
	}
	else
	{
		// recursive case, generate union w/ 3 children
		DrgPexpr *pdrgpexprInput = GPOS_NEW(pmp) DrgPexpr(pmp, 3);
		DrgDrgPcr *pdrgpdrgpcrInput = GPOS_NEW(pmp) DrgDrgPcr(pmp);

		for (ULONG i = 0; i < 3; i++)
		{
			CExpression *pexprInput = PexprLogicalUnion(pmp, ulDepth -1);
			COperator *pop = pexprInput->Pop();
			DrgPcr *pdrgpcr = NULL;

			if (pop->Eopid() == COperator::EopLogicalGet)
			{
				CLogicalGet *popGet =  CLogicalGet::PopConvert(pop);
				pdrgpcr = popGet->PdrgpcrOutput();
			}
			else
			{
				GPOS_ASSERT(COperator::EopLogicalUnion == pop->Eopid());
				CLogicalUnion *popUnion = CLogicalUnion::PopConvert(pop);
				pdrgpcr = popUnion->PdrgpcrOutput();
			}
			pdrgpexprInput->Append(pexprInput);
			GPOS_ASSERT(NULL != pdrgpcr);

			pdrgpcr->AddRef();
			pdrgpdrgpcrInput->Append(pdrgpcr);
		}

		// remap columns of first input
		DrgPcr *pdrgpcrInput = (*pdrgpdrgpcrInput)[0];
		ULONG ulCols = pdrgpcrInput->UlLength();
		DrgPcr *pdrgpcrOutput = GPOS_NEW(pmp) DrgPcr(pmp, ulCols);

		for (ULONG j = 0; j < ulCols; j++)
		{
			pdrgpcrOutput->Append((*pdrgpcrInput)[j]);
		}

		pexpr = GPOS_NEW(pmp) CExpression
							(
							pmp,
							GPOS_NEW(pmp) CLogicalUnion(pmp, pdrgpcrOutput, pdrgpdrgpcrInput),
							pdrgpexprInput
							);
	}

	return pexpr;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalSequenceProject
//
//	@doc:
//		Generate a logical sequence project expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalSequenceProject
	(
	IMemoryPool *pmp,
	OID oidFunc,
	CExpression *pexprInput
	)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	CColumnFactory *pcf = COptCtxt::PoctxtFromTLS()->Pcf();

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(CUtils::PexprScalarIdent(pmp, CDrvdPropRelational::Pdprel(pexprInput->PdpDerive())->PcrsOutput()->PcrAny()));
	DrgPos *pdrgpos = GPOS_NEW(pmp) DrgPos(pmp);
	DrgPwf *pdrgwf = GPOS_NEW(pmp) DrgPwf(pmp);

	CLogicalSequenceProject *popSeqProj =
			GPOS_NEW(pmp) CLogicalSequenceProject
				(
				pmp,
				GPOS_NEW(pmp) CDistributionSpecHashed(pdrgpexpr, true /*fNullsCollocated*/),
				pdrgpos,
				pdrgwf
				);

	IMDId *pmdid = GPOS_NEW(pmp) CMDIdGPDB(oidFunc);
	const IMDFunction *pmdfunc = pmda->Pmdfunc(pmdid);

	IMDId *pmdidRetType = pmdfunc->PmdidTypeResult();
	pmdidRetType->AddRef();

	CExpression *pexprWinFunc =
			GPOS_NEW(pmp) CExpression
				(
				pmp,
				GPOS_NEW(pmp) CScalarWindowFunc
							(
							pmp,
							pmdid,
							pmdidRetType,
							GPOS_NEW(pmp) CWStringConst(pmp, pmdfunc->Mdname().Pstr()->Wsz()),
							CScalarWindowFunc::EwsImmediate,
							false /*fDistinct*/
							)
				);
	CColRef *pcrComputed = pcf->PcrCreate(pmda->Pmdtype(pmdfunc->PmdidTypeResult()));

	CExpression *pexprPrjList =
		GPOS_NEW(pmp) CExpression
			(
			pmp,
			GPOS_NEW(pmp) CScalarProjectList(pmp),
			CUtils::PexprScalarProjectElement(pmp, pcrComputed, pexprWinFunc)
			);

	return GPOS_NEW(pmp) CExpression(pmp, popSeqProj, pexprInput, pexprPrjList);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprOneWindowFunction
//
//	@doc:
//		Generate a random expression with one window function
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprOneWindowFunction
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprGet = PexprLogicalGet(pmp);

	return PexprLogicalSequenceProject(pmp, GPDB_WIN_ROW_NUMBER, pexprGet);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprTwoWindowFunctions
//
//	@doc:
//		Generate a random expression with two window functions
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprTwoWindowFunctions
	(
	IMemoryPool *pmp
	)
{
	CExpression *pexprWinFunc = PexprOneWindowFunction(pmp);

	return PexprLogicalSequenceProject(pmp, GPDB_WIN_RANK, pexprWinFunc);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PdrgpexprJoins
//
//	@doc:
//		Generate an array of join expressions for the given relation names;
//		the first generated expression is a Select of the first relation;
//		every subsequent expression is a Join between a new relation and the
//		previous join expression
//
//---------------------------------------------------------------------------
DrgPexprJoins *
CTestUtils::PdrgpexprJoins
	(
	IMemoryPool *pmp,
	CWStringConst *pstrRel, // array of relation names
	ULONG *pulRel, // array of relation oids
	ULONG ulRels, // number of relations
	BOOL fCrossProduct
	)
{
	DrgPexprJoins *pdrgpexpr = GPOS_NEW(pmp) DrgPexprJoins(pmp);
	for (ULONG i = 0; i < ulRels; i++)
	{
		CExpression *pexpr = NULL;
		if (fCrossProduct)
		{
			pexpr = PexprLogicalGet(pmp, &pstrRel[i], &pstrRel[i], pulRel[i]);
		}
		else
		{
			pexpr = PexprLogicalSelect(pmp, &pstrRel[i], &pstrRel[i], pulRel[i]);
		}


		if (0 == i)
		{
			// the first join is set to a Select/Get of the first relation
			pdrgpexpr->Append(pexpr);
		}
		else
		{
			CExpression *pexprJoin = NULL;
			if (fCrossProduct)
			{
				// create a cross product
				pexprJoin = CUtils::PexprLogicalJoin<CLogicalInnerJoin>
					(
					pmp,
					(*pdrgpexpr)[i - 1],
					pexpr,
					CPredicateUtils::PexprConjunction(pmp, NULL) // generate a constant True
				);
			}
			else
			{
				// otherwise, we create a new join out of the last created
				// join and the current Select expression
				pexprJoin = PexprLogicalJoin<CLogicalInnerJoin>(pmp, (*pdrgpexpr)[i - 1], pexpr);
			}

			pdrgpexpr->Append(pexprJoin);
		}

		GPOS_CHECK_ABORT;
	}

	return pdrgpexpr;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprLogicalNAryJoin
//
//	@doc:
//		Generate an n-ary join expression using given array of relation names
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprLogicalNAryJoin
	(
	IMemoryPool *pmp,
	CWStringConst *pstrRel,
	ULONG *pulRel,
	ULONG ulRels,
	BOOL fCrossProduct
	)
{
	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	for (ULONG ul = 0; ul < ulRels; ul++)
	{
		CExpression *pexpr = PexprLogicalGet(pmp, &pstrRel[ul], &pstrRel[ul], pulRel[ul]);
		pdrgpexpr->Append(pexpr);
	}

	DrgPexpr *pdrgpexprPred = NULL;
	if (!fCrossProduct)
	{
		pdrgpexprPred = GPOS_NEW(pmp) DrgPexpr(pmp);
		for (ULONG ul = 0; ul < ulRels - 1; ul++)
		{
			CExpression *pexprLeft = (*pdrgpexpr)[ul];
			CExpression *pexprRight = (*pdrgpexpr)[ul + 1];

			// get any two columns; one from each side
			CColRef *pcrLeft = CDrvdPropRelational::Pdprel(pexprLeft->PdpDerive())->PcrsOutput()->PcrAny();
			CColRef *pcrRight = CDrvdPropRelational::Pdprel(pexprRight->PdpDerive())->PcrsOutput()->PcrAny();
			pdrgpexprPred->Append(CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight));
		}
	}
	pdrgpexpr->Append(CPredicateUtils::PexprConjunction(pmp, pdrgpexprPred));

	return PexprLogicalNAryJoin(pmp, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PqcGenerate
//
//	@doc:
//		Generate a dummy context from an array of column references and
//		empty sort columns for testing
//
//---------------------------------------------------------------------------
CQueryContext *
CTestUtils::PqcGenerate
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	DrgPcr *pdrgpcr
	)
{
	// generate required columns
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(pdrgpcr);

	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	CDistributionSpec *pds =
		GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);

	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);

	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);

	// we require exact matching on distribution since final query results must be sent to master
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);

	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);
		
	CCTEReq *pcter = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PcterProducers(pmp);

	CReqdPropPlan *prpp = GPOS_NEW(pmp) CReqdPropPlan(pcrs, peo, ped, per, pcter);

	pdrgpcr->AddRef();
	DrgPmdname *pdrgpmdname = GPOS_NEW(pmp) DrgPmdname(pmp);
	const ULONG ulLen = pdrgpcr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pcr->Name().Pstr());
		pdrgpmdname->Append(pmdname);
	}

	return GPOS_NEW(pmp) CQueryContext(pmp, pexpr, prpp, pdrgpcr, pdrgpmdname, true /*fDeriveStats*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PqcGenerate
//
//	@doc:
//		Generate a dummy context with a subset of required column, one
//		sort column and one distribution column
//
//---------------------------------------------------------------------------
CQueryContext *
CTestUtils::PqcGenerate
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	CColRefSet *pcrs = GPOS_NEW(pmp) CColRefSet(pmp);
	pcrs->Include(CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput());

	// keep a subset of columns
	CColRefSet *pcrsOutput = GPOS_NEW(pmp) CColRefSet(pmp);
	CColRefSetIter crsi(*pcrs);
	while (crsi.FAdvance())
	{
		CColRef *pcr = crsi.Pcr();
		if (1 != pcr->UlId() % GPOPT_TEST_REL_WIDTH)
		{
			pcrsOutput->Include(pcr);
		}
	}
	pcrs->Release();

	// construct an ordered array of the output columns
	DrgPcr *pdrgpcr = GPOS_NEW(pmp) DrgPcr(pmp);
	CColRefSetIter crsiOutput(*pcrsOutput);
	while (crsiOutput.FAdvance())
	{
		CColRef *pcr = crsiOutput.Pcr();
		pdrgpcr->Append(pcr);
	}

	// generate a sort order
	COrderSpec *pos = GPOS_NEW(pmp) COrderSpec(pmp);
	pos->Append(GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_LT_OP), pcrsOutput->PcrAny(), COrderSpec::EntFirst);

	CDistributionSpec *pds =
		GPOS_NEW(pmp) CDistributionSpecSingleton(CDistributionSpecSingleton::EstMaster);

	CRewindabilitySpec *prs = GPOS_NEW(pmp) CRewindabilitySpec(CRewindabilitySpec::ErtNone /*ert*/);

	CEnfdOrder *peo = GPOS_NEW(pmp) CEnfdOrder(pos, CEnfdOrder::EomSatisfy);

	// we require exact matching on distribution since final query results must be sent to master
	CEnfdDistribution *ped = GPOS_NEW(pmp) CEnfdDistribution(pds, CEnfdDistribution::EdmExact);

	CEnfdRewindability *per = GPOS_NEW(pmp) CEnfdRewindability(prs, CEnfdRewindability::ErmSatisfy);

	CCTEReq *pcter = COptCtxt::PoctxtFromTLS()->Pcteinfo()->PcterProducers(pmp);

	CReqdPropPlan *prpp = GPOS_NEW(pmp) CReqdPropPlan(pcrsOutput, peo, ped, per, pcter);

	DrgPmdname *pdrgpmdname = GPOS_NEW(pmp) DrgPmdname(pmp);
	const ULONG ulLen = pdrgpcr->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		CColRef *pcr = (*pdrgpcr)[ul];
		CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pcr->Name().Pstr());
		pdrgpmdname->Append(pmdname);
	}

	return GPOS_NEW(pmp) CQueryContext(pmp, pexpr, prpp, pdrgpcr, pdrgpmdname, true /*fDeriveStats*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprScalarNestedPreds
//
//	@doc:
//		Helper for generating a nested AND/OR/NOT tree
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprScalarNestedPreds
	(
	IMemoryPool *pmp,
	CExpression *pexpr,
	CScalarBoolOp::EBoolOperator eboolop
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);
	GPOS_ASSERT(CScalarBoolOp::EboolopAnd == eboolop ||
				CScalarBoolOp::EboolopOr == eboolop ||
				CScalarBoolOp::EboolopNot == eboolop);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRef *pcrLeft =  pcrs->PcrAny();
	CExpression *pexprConstFst = CUtils::PexprScalarConstInt4(pmp, 3 /*iVal*/);

	CExpression *pexprPredFst = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pexprConstFst);
	CExpression *pexprPredSnd = NULL;

	if (CScalarBoolOp::EboolopNot != eboolop)
	{
		CExpression *pexprConstSnd = CUtils::PexprScalarConstInt4(pmp, 5 /*iVal*/);
		pexprPredSnd = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pexprConstSnd);
	}

	DrgPexpr *pdrgpexprFst = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexprFst->Append(pexprPredFst);

	if (CScalarBoolOp::EboolopNot != eboolop)
	{
		pdrgpexprFst->Append(pexprPredSnd);
	}

	CExpression *pexprBoolOp = CUtils::PexprScalarBoolOp(pmp, eboolop , pdrgpexprFst);
	DrgPexpr *pdrgpexprSnd = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexprSnd->Append(pexprBoolOp);

	if (CScalarBoolOp::EboolopNot != eboolop)
	{
		pexprPredSnd->AddRef();
		pdrgpexprSnd->Append(pexprPredSnd);
	}

	return CUtils::PexprScalarBoolOp(pmp, eboolop , pdrgpexprSnd);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EqualityPredicate
//
//	@doc:
//		Generate equality predicate given column sets
//
//---------------------------------------------------------------------------
void
CTestUtils::EqualityPredicate
	(
	IMemoryPool *pmp,
	CColRefSet *pcrsLeft,
	CColRefSet *pcrsRight,
	DrgPexpr *pdrgpexpr
	)
{
	CColRef *pcrLeft = pcrsLeft->PcrAny();
	CColRef *pcrRight = pcrsRight->PcrAny();

	// generate correlated predicate
	CExpression *pexprCorrelation = CUtils::PexprScalarEqCmp(pmp, pcrLeft, pcrRight);

	pdrgpexpr->Append(pexprCorrelation);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PpointInt2
//
//	@doc:
//		Create an INT2 point
//
//---------------------------------------------------------------------------
CPoint *
CTestUtils::PpointInt2
	(
	IMemoryPool *pmp,
	INT i
	)
{
	CDatumInt2GPDB *pdatum = GPOS_NEW(pmp) CDatumInt2GPDB(m_sysidDefault, (SINT) i);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	return ppoint;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PpointInt4
//
//	@doc:
//		Create an INT4 point
//
//---------------------------------------------------------------------------
CPoint *
CTestUtils::PpointInt4
	(
	IMemoryPool *pmp,
	INT i
	)
{
	CDatumInt4GPDB *pdatum = GPOS_NEW(pmp) CDatumInt4GPDB(m_sysidDefault, i);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	return ppoint;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PpointInt4NullVal
//
//	@doc:
//		Create an INT4 point with null value
//
//---------------------------------------------------------------------------
CPoint *
CTestUtils::PpointInt4NullVal
	(
	IMemoryPool *pmp
	)
{
	CDatumInt4GPDB *pdatum = GPOS_NEW(pmp) CDatumInt4GPDB(m_sysidDefault, 0, true /* fNull */);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	return ppoint;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PpointInt8
//
//	@doc:
//		Create an INT8 point
//
//---------------------------------------------------------------------------
CPoint *
CTestUtils::PpointInt8
	(
	IMemoryPool *pmp,
	INT i
	)
{
	CDatumInt8GPDB *pdatum = GPOS_NEW(pmp) CDatumInt8GPDB(m_sysidDefault, (LINT) i);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	return ppoint;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PpointBool
//
//	@doc:
//		Create a point
//
//---------------------------------------------------------------------------
CPoint *
CTestUtils::PpointBool
	(
	IMemoryPool *pmp,
	BOOL fVal
	)
{
	CDatumBoolGPDB *pdatum = GPOS_NEW(pmp) CDatumBoolGPDB(m_sysidDefault, fVal);
	CPoint *ppoint = GPOS_NEW(pmp) CPoint(pdatum);
	return ppoint;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprReadQuery
//
//	@doc:
//		Return query logical expression from minidump
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprReadQuery
	(
	IMemoryPool *pmp,
	const CHAR *szQueryFileName
	)
{
	CHAR *szQueryDXL = CDXLUtils::SzRead(pmp, szQueryFileName);

	// parse the DXL query tree from the given DXL document
	CQueryToDXLResult *ptroutput = CDXLUtils::PdxlnParseDXLQuery(pmp, szQueryDXL, NULL);

	// get md accessor
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	GPOS_ASSERT(NULL != pmda);

	// translate DXL tree into CExpression
	CTranslatorDXLToExpr trdxl2expr(pmp, pmda);
	CExpression *pexprQuery =
			trdxl2expr.PexprTranslateQuery
						(
						ptroutput->Pdxln(),
						ptroutput->PdrgpdxlnOutputCols(),
						ptroutput->PdrgpdxlnCTE()
						);

	GPOS_DELETE(ptroutput);
	GPOS_DELETE_ARRAY(szQueryDXL);

	return pexprQuery;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresTranslate
//
//	@doc:
//		Rehydrate a query from the given file, optimize it, translate back to
//		DXL and compare the resulting plan to the given DXL document
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresTranslate
	(
	IMemoryPool *pmp,
	const CHAR *szQueryFileName,
	const CHAR *szPlanFileName,
	BOOL fIgnoreMismatch
	)
{
	// debug print
	CWStringDynamic str(pmp);
	COstreamString oss(&str);

	GPOS_TRACE(str.Wsz());

	// read the dxl document
	CHAR *szQueryDXL = CDXLUtils::SzRead(pmp, szQueryFileName);

	// parse the DXL query tree from the given DXL document
	CQueryToDXLResult *ptroutput = CDXLUtils::PdxlnParseDXLQuery(pmp, szQueryDXL, NULL);

	// get md accessor
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	GPOS_ASSERT(NULL != pmda);

	// translate DXL tree into CExpression
	CTranslatorDXLToExpr ptrdxl2expr(pmp, pmda);
	CExpression *pexprQuery = ptrdxl2expr.PexprTranslateQuery
											(
											ptroutput->Pdxln(),
											ptroutput->PdrgpdxlnOutputCols(),
											ptroutput->PdrgpdxlnCTE()
											);

	CQueryContext *pqc = CQueryContext::PqcGenerate
												(
												pmp,
												pexprQuery,
												ptrdxl2expr.PdrgpulOutputColRefs(),
												ptrdxl2expr.Pdrgpmdname(),
												true /*fDeriveStats*/
												);

#ifdef GPOS_DEBUG
	pqc->OsPrint(oss);
#endif //GPOS_DEBUG

	gpopt::CEngine eng(pmp);
	eng.Init(pqc, NULL /*pdrgpss*/);

#ifdef GPOS_DEBUG
	eng.RecursiveOptimize();
#else
	eng.Optimize();
#endif //GPOS_DEBUG

	gpopt::CExpression *pexprPlan = eng.PexprExtractPlan();
	GPOS_ASSERT(NULL != pexprPlan);

	(void) pexprPlan->PrppCompute(pmp, pqc->Prpp());
	pexprPlan->OsPrint(oss);

	// translate plan back to DXL
	CTranslatorExprToDXL ptrexpr2dxl(pmp, pmda, PdrgpiSegments(pmp));
	CDXLNode *pdxlnPlan = ptrexpr2dxl.PdxlnTranslate(pexprPlan, pqc->PdrgPcr(), pqc->Pdrgpmdname());
	GPOS_ASSERT(NULL != pdxlnPlan);

	COptimizerConfig *poconf = COptCtxt::PoctxtFromTLS()->Poconf();

	CWStringDynamic *pstrTranslatedPlan =
		CDXLUtils::PstrSerializePlan(pmp, pdxlnPlan, poconf->Pec()->UllPlanId(), poconf->Pec()->UllPlanSpaceSize(), true /*fSerializeHeaderFooter*/, true /*fIndent*/);

	GPOS_TRACE(str.Wsz());
	GPOS_RESULT eres = GPOS_OK;
	if (NULL != szPlanFileName)
	{
		// parse the DXL plan tree from the given DXL file
		CHAR *szExpectedPlan = CDXLUtils::SzRead(pmp, szPlanFileName);

		CWStringDynamic strExpectedPlan(pmp);
		strExpectedPlan.AppendFormat(GPOS_WSZ_LIT("%s"), szExpectedPlan);

		eres = EresCompare(oss, pstrTranslatedPlan, &strExpectedPlan, fIgnoreMismatch);
		GPOS_DELETE_ARRAY(szExpectedPlan);
	}

	// cleanup
	pexprQuery->Release();
	pexprPlan->Release();
	pdxlnPlan->Release();
	GPOS_DELETE_ARRAY(szQueryDXL);
	GPOS_DELETE(pstrTranslatedPlan);
	GPOS_DELETE(ptroutput);
	GPOS_DELETE(pqc);
	return eres;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresCompare
//
//	@doc:
//		Compare expected and actual output
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresCompare
	(
	IOstream &os,
	CWStringDynamic *pstrActual,
	CWStringDynamic *pstrExpected,
	BOOL fIgnoreMismatch
	)
{
	if (!pstrExpected->FEquals(pstrActual))
	{
		os << "Output does not match expected DXL document" << std::endl;
		os << "Actual: " << std::endl;
		os << pstrActual->Wsz() << std::endl;

		os << "Expected: " << std::endl;
		os << pstrExpected->Wsz() << std::endl;

		if (fIgnoreMismatch)
		{
			return GPOS_OK;
		}
		return GPOS_FAILED;
	}
	else
	{
		os << "Output matches expected DXL document" << std::endl;
		return GPOS_OK;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::FPlanMatch
//
//	@doc:
//		Match given two plans using string comparison
//
//---------------------------------------------------------------------------
BOOL
CTestUtils::FPlanMatch
	(
	IMemoryPool *pmp,
	IOstream &os,
	const CDXLNode *pdxlnFst,
	ULLONG ullPlanIdFst,
	ULLONG ullPlanSpaceSizeFst,
	const CDXLNode *pdxlnSnd,
	ULLONG ullPlanIdSnd,
	ULLONG ullPlanSpaceSizeSnd
	)
{
	if (NULL == pdxlnFst && NULL == pdxlnSnd)
	{
		CAutoTrace at(pmp);
		at.Os() << "Both plans are NULL." << std::endl;

		return true;
	}

	if (NULL != pdxlnFst && NULL == pdxlnSnd)
	{
		CAutoTrace at(pmp);
		at.Os() << "Second plan is NULL. First: " << std::endl;
		CWStringDynamic *pstrFst = CDXLUtils::PstrSerializePlan(pmp, pdxlnFst, ullPlanIdFst, ullPlanSpaceSizeFst, false /*fDocumentHeaderFooter*/, true /*fIndent*/);
		at.Os()  << pstrFst->Wsz() << std::endl;

		GPOS_DELETE(pstrFst);

		return false;
	}

	if (NULL == pdxlnFst && NULL != pdxlnSnd)
	{
		CAutoTrace at(pmp);
		at.Os()  << "First plan is NULL. Second: " << std::endl;
		CWStringDynamic *pstrSnd = CDXLUtils::PstrSerializePlan(pmp, pdxlnSnd, ullPlanIdSnd, ullPlanSpaceSizeSnd, false /*fDocumentHeaderFooter*/, true /*fIndent*/);
		at.Os()  << pstrSnd->Wsz() << std::endl;

		GPOS_DELETE(pstrSnd);

		return false;
	}		
		
	GPOS_ASSERT(NULL != pdxlnFst);
	GPOS_ASSERT(NULL != pdxlnSnd);
	
	// plan id's and space sizes are already compared before this point,
	// overwrite PlanId's and space sizes with zeros to pass string comparison on plan body
	CWStringDynamic *pstrFst = CDXLUtils::PstrSerializePlan(pmp, pdxlnFst, 0 /*ullPlanIdFst*/, 0 /*ullPlanSpaceSizeFst*/, false /*fDocumentHeaderFooter*/, true /*fIndent*/);
	GPOS_CHECK_ABORT;

	CWStringDynamic *pstrSnd = CDXLUtils::PstrSerializePlan(pmp, pdxlnSnd, 0 /*ullPlanIdSnd*/, 0 /*ullPlanSpaceSizeSnd*/, false /*fDocumentHeaderFooter*/, true /*fIndent*/);
	GPOS_CHECK_ABORT;
	
	BOOL fResult = pstrFst->FEquals(pstrSnd);

	// cleanup
	GPOS_DELETE(pstrFst);
	GPOS_DELETE(pstrSnd);

	if (!fResult)
	{
		// serialize plans again to restore id's and space size before printing error message
		CWStringDynamic *pstrFst = CDXLUtils::PstrSerializePlan(pmp, pdxlnFst, ullPlanIdFst, ullPlanSpaceSizeFst, false /*fDocumentHeaderFooter*/, true /*fIndent*/);
		GPOS_CHECK_ABORT;

		CWStringDynamic *pstrSnd = CDXLUtils::PstrSerializePlan(pmp, pdxlnSnd, ullPlanIdSnd, ullPlanSpaceSizeSnd, false /*fDocumentHeaderFooter*/, true /*fIndent*/);
		GPOS_CHECK_ABORT;

		{
			CAutoTrace at(pmp);

			at.Os() << "First: " << std::endl;
			at.Os()  << pstrFst->Wsz() << std::endl;
		}

		os << "Second: " << std::endl;
		os << pstrSnd->Wsz() << std::endl;

		// cleanup
		GPOS_DELETE(pstrFst);
		GPOS_DELETE(pstrSnd);
	}
	
	
	GPOS_CHECK_ABORT;
	return fResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::FPlanCompare
//
//	@doc:
//		Compare two plans based on their string representation
//
//---------------------------------------------------------------------------
BOOL
CTestUtils::FPlanCompare
	(
	IMemoryPool *pmp,
	IOstream &os,
	const CDXLNode *pdxlnFst,
	ULLONG ullPlanIdFst,
	ULLONG ullPlanSpaceSizeFst,
	const CDXLNode *pdxlnSnd,
	ULLONG ullPlanIdSnd,
	ULLONG ullPlanSpaceSizeSnd,
	BOOL fMatchPlans,
	INT iCmpSpaceSize
	)
{
	if (!fMatchPlans)
	{
		return true;
	}

	CAutoTrace at(pmp);

	if (ullPlanIdFst != ullPlanIdSnd)
	{
		at.Os()
		<< "Plan Id mismatch." << std::endl
		<< "\tCurrent Id: " << ullPlanIdFst << std::endl
		<< "\tExpected Id: " << ullPlanIdSnd << std::endl;

		return false;
	}

	// check plan space size required comparison
	if (
		(0 == iCmpSpaceSize && ullPlanSpaceSizeFst != ullPlanSpaceSizeSnd) ||	// required comparison is equality
		(-1 == iCmpSpaceSize && ullPlanSpaceSizeFst > ullPlanSpaceSizeSnd) ||	// required comparison is (Fst <= Snd)
		(1 == iCmpSpaceSize && ullPlanSpaceSizeFst < ullPlanSpaceSizeSnd)  // required comparison is (Fst >= Snd)
		)
	{
		at.Os()
				<< "Plan space size comparison failed." << std::endl
				<< "Required comparison: " << iCmpSpaceSize << std::endl
				<< "\tCurrent size: " << ullPlanSpaceSizeFst << std::endl
				<< "\tExpected size: " << ullPlanSpaceSizeSnd << std::endl;

		return false;
	}

	// perform deep matching on plan bodies
	return FPlanMatch(pmp, os, pdxlnFst, ullPlanIdFst, ullPlanSpaceSizeFst, pdxlnSnd, ullPlanIdSnd, ullPlanSpaceSizeSnd);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PdrgpiSegments
//
//	@doc:
//		Helper function for creating an array of segment ids for the target system
//
//---------------------------------------------------------------------------
DrgPi *
CTestUtils::PdrgpiSegments
	(
	IMemoryPool *pmp
	)
{
	DrgPi *pdrgpiSegments = GPOS_NEW(pmp) DrgPi(pmp);
	const ULONG ulSegments = GPOPT_SEGMENT_COUNT;
	GPOS_ASSERT(0 < ulSegments);

	for (ULONG ul = 0; ul < ulSegments; ul++)
	{
		pdrgpiSegments->Append(GPOS_NEW(pmp) INT(ul));
	}
	return pdrgpiSegments;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::FFaultSimulation
//
//	@doc:
//		Check if we are in fault simulation mode
//
//---------------------------------------------------------------------------
BOOL
CTestUtils::FFaultSimulation()
{
	return GPOS_FTRACE(EtraceSimulateAbort) 
			|| GPOS_FTRACE(EtraceSimulateIOError) 
			|| GPOS_FTRACE(EtraceSimulateOOM) 
			|| IWorker::m_fEnforceTimeSlices;
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::SzMinidumpFileName
//
//	@doc:
//		Generate minidump file name from passed file name
//
//---------------------------------------------------------------------------
CHAR *
CTestUtils::SzMinidumpFileName
	(
	IMemoryPool *pmp,
	const CHAR *szFileName
	)
{
	GPOS_ASSERT(NULL != szFileName);

	if (!GPOS_FTRACE(EopttraceEnableSpacePruning))
	{
		return const_cast<CHAR *>(szFileName);
	}

	CWStringDynamic *pstrMinidumpFileName = GPOS_NEW(pmp) CWStringDynamic(pmp);
	COstreamString oss(pstrMinidumpFileName);
	oss << szFileName << "-space-pruned";

	// convert wide char to regular char
	const WCHAR *wsz = pstrMinidumpFileName->Wsz();
	const ULONG ulInputLength = GPOS_WSZ_LENGTH(wsz);
	const ULONG ulWCHARSize = GPOS_SIZEOF(WCHAR);
	const ULONG ulMaxLength = (ulInputLength + 1) * ulWCHARSize;
	CHAR *sz = GPOS_NEW_ARRAY(pmp, CHAR, ulMaxLength);
	gpos::clib::LWcsToMbs(sz, const_cast<WCHAR *>(wsz), ulMaxLength);
	sz[ulMaxLength - 1] = '\0';

	GPOS_DELETE(pstrMinidumpFileName);

	return sz;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresRunMinidump
//
//	@doc:
//		Run one minidump-based test using passed MD Accessor
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresRunMinidump
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	const CHAR *szFileName,
	ULONG *pulTestCounter,
	ULONG ulSessionId,
	ULONG ulCmdId,
	BOOL fMatchPlans,
	INT iCmpSpaceSize,
	IConstExprEvaluator *pceeval
	)
{
	GPOS_ASSERT(NULL != pmda);

	GPOS_RESULT eres = GPOS_OK;

	{
		CAutoTrace at(pmp);
		at.Os() << "executing " << szFileName;
	}

	// load dump file
	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, szFileName);
	GPOS_CHECK_ABORT;

	COptimizerConfig *poconf = pdxlmd->Poconf();

	if (NULL == poconf)
	{
		poconf = COptimizerConfig::PoconfDefault(pmp);
	}
	else
	{
		poconf->AddRef();
	}

	ULONG ulSegments = UlSegments(poconf);

	// allow sampler to throw invalid plan exception
	poconf->Pec()->SetSampleValidPlans(false /*fSampleValidPlans*/);

	CDXLNode *pdxlnPlan = NULL;

	CHAR *szMinidumpFileName = SzMinidumpFileName(pmp, szFileName);

	pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
					(
					pmp,
					pmda,
					pdxlmd,
					szMinidumpFileName,
					ulSegments,
					ulSessionId,
					ulCmdId,
					poconf,
					pceeval
					);

	if (szMinidumpFileName != szFileName)
	{
		// a new name was generated
		GPOS_DELETE_ARRAY(szMinidumpFileName);
	}

	GPOS_CHECK_ABORT;


	{
		CAutoTrace at(pmp);
		if (!CTestUtils::FPlanCompare
			(
			pmp,
			at.Os(),
			pdxlnPlan,
			poconf->Pec()->UllPlanId(),
			poconf->Pec()->UllPlanSpaceSize(),
			pdxlmd->PdxlnPlan(),
			pdxlmd->UllPlanId(),
			pdxlmd->UllPlanSpaceSize(),
			fMatchPlans,
			iCmpSpaceSize
			)
		)
		{
			eres = GPOS_FAILED;
		}
    }

	GPOS_CHECK_ABORT;

	{
		CAutoTrace at(pmp);
		at.Os() << std::endl;
	}

	// cleanup
	GPOS_DELETE(pdxlmd);
	pdxlnPlan->Release();
	poconf->Release();

	(*pulTestCounter)++;
	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresRunMinidumps
//
//	@doc:
//		Run minidump-based tests
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresRunMinidumps
	(
	IMemoryPool *, // pmpInput,
	const CHAR *rgszFileNames[],
	ULONG ulTests,
	ULONG *pulTestCounter,
	ULONG ulSessionId,
	ULONG ulCmdId,
	BOOL fMatchPlans,
	BOOL fTestSpacePruning,
	const CHAR *, // szMDFilePath,
	IConstExprEvaluator *pceeval
	)
{
	GPOS_RESULT eres = GPOS_OK;
	
	for (ULONG ul = *pulTestCounter; eres == GPOS_OK && ul < ulTests; ul++)
	{
		// each test uses a new memory pool to keep total memory consumption low
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();

		if (fMatchPlans)
		{
			{
				CAutoTrace at(pmp);
				at.Os() << "Running test with EXHAUSTIVE SEARCH:";
			}

			eres = EresRunMinidumpsUsingOneMDFile
				(
				pmp,
				rgszFileNames[ul],
				&rgszFileNames[ul],
				1, // ulTests
				pulTestCounter,
				ulSessionId,
				ulCmdId,
				fMatchPlans,
				0, // iCmpSpaceSize
				pceeval
				);
		}

		if (GPOS_OK == eres && fTestSpacePruning)
		{

			{
				CAutoTrace at(pmp);
				at.Os() << "Running test with BRANCH-AND-BOUND SEARCH:";
			}

			// enable space pruning
			CAutoTraceFlag atf(EopttraceEnableSpacePruning, true /*fVal*/);

			eres = EresRunMinidumpsUsingOneMDFile
				(
				pmp,
				rgszFileNames[ul],
				&rgszFileNames[ul],
				1, // ulTests
				pulTestCounter,
				ulSessionId,
				ulCmdId,
				fMatchPlans,
				-1, // iCmpSpaceSize
				pceeval
				);
		}
	}

	*pulTestCounter = 0;
	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresRunMinidumpsUsingOneMDFile
//
//	@doc:
//		Run all minidumps based on one metadata file
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresRunMinidumpsUsingOneMDFile
	(
	IMemoryPool *pmp,
	const CHAR *szMDFilePath,
	const CHAR *rgszFileNames[],
	ULONG ulTests,
	ULONG *pulTestCounter,
	ULONG ulSessionId,
	ULONG ulCmdId,
	BOOL fMatchPlans,
	INT iCmpSpaceSize,
	IConstExprEvaluator *pceeval
	)
{
	GPOS_ASSERT(NULL != rgszFileNames);
	GPOS_ASSERT(NULL != szMDFilePath);

	// reset metadata cache
	CMDCache::Reset();

	// load metadata file
	CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, szMDFilePath);
	GPOS_CHECK_ABORT;

	// set up MD providers
	CMDProviderMemory *pmdp = GPOS_NEW(pmp) CMDProviderMemory(pmp, szMDFilePath);
	GPOS_CHECK_ABORT;

	const DrgPsysid *pdrgpsysid = pdxlmd->Pdrgpsysid();
	DrgPmdp *pdrgpmdp = GPOS_NEW(pmp) DrgPmdp(pmp);
	pdrgpmdp->Append(pmdp);

	for (ULONG ul = 1; ul < pdrgpsysid->UlLength(); ul++)
	{
		pmdp->AddRef();
		pdrgpmdp->Append(pmdp);
	}

	GPOS_RESULT eres = GPOS_OK;

	{	// scope for MD accessor
		CMDAccessor mda(pmp, CMDCache::Pcache(), pdrgpsysid, pdrgpmdp);

		for (ULONG ul = *pulTestCounter; eres == GPOS_OK && ul < ulTests; ul++)
		{
			eres = EresRunMinidump(pmp, &mda, rgszFileNames[ul], pulTestCounter, ulSessionId, ulCmdId, fMatchPlans, iCmpSpaceSize, pceeval);
		}

		*pulTestCounter = 0;
	}

	// cleanup
	pdrgpmdp->Release();
	GPOS_DELETE(pdxlmd);
	GPOS_CHECK_ABORT;

	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresSamplePlans
//
//	@doc:
//		Test plan sampling
//		to extract attribute 'X' value from xml file:
//		xpath distr.xml //dxl:Value/@X | grep 'X=' | sed 's/\"//g' | sed 's/X=//g' | tr ' ' '\n'
//
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresSamplePlans
	(
	const CHAR *rgszFileNames[],
	ULONG ulTests,
	ULONG *pulTestCounter,
	ULONG ulSessionId,
	ULONG ulCmdId
	)
{
	GPOS_RESULT eres = GPOS_OK;

	for (ULONG ul = *pulTestCounter; ul < ulTests; ul++)
	{
		// each test uses a new memory pool to keep total memory consumption low
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();

		// reset metadata cache
		CMDCache::Reset();

		CAutoTraceFlag atf1(EopttraceEnumeratePlans, true);
		CAutoTraceFlag atf2(EopttraceSamplePlans, true);

		// load dump file
		CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, rgszFileNames[ul]);
		GPOS_CHECK_ABORT;

		// set up MD providers
		CMDProviderMemory *pmdp = GPOS_NEW(pmp) CMDProviderMemory(pmp, rgszFileNames[ul]);
		GPOS_CHECK_ABORT;

		const DrgPsysid *pdrgpsysid = pdxlmd->Pdrgpsysid();
		DrgPmdp *pdrgpmdp = GPOS_NEW(pmp) DrgPmdp(pmp);
		pdrgpmdp->Append(pmdp);

		for (ULONG ulSys = 1; ulSys < pdrgpsysid->UlLength(); ulSys++)
		{
			pmdp->AddRef();
			pdrgpmdp->Append(pmdp);
		}

		COptimizerConfig *poconf = pdxlmd->Poconf();
			
		if (NULL == poconf)
		{
			poconf = GPOS_NEW(pmp) COptimizerConfig
								(
								GPOS_NEW(pmp) CEnumeratorConfig(pmp, 0 /*ullPlanId*/, 1000 /*ullSamples*/),
								CStatisticsConfig::PstatsconfDefault(pmp),
								CCTEConfig::PcteconfDefault(pmp),
								ICostModel::PcmDefault(pmp),
								CHint::PhintDefault(pmp)
								);
		}
		else
		{
			poconf->AddRef();
		}

		ULONG ulSegments = UlSegments(poconf);

		// allow sampler to throw invalid plan exception
		poconf->Pec()->SetSampleValidPlans(false /*fSampleValidPlans*/);

		{
			// scope for MD accessor
			CMDAccessor mda(pmp, CMDCache::Pcache(), pdrgpsysid, pdrgpmdp);

			CDXLNode *pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
							(
							pmp,
							&mda,
							pdxlmd,
							rgszFileNames[ul],
							ulSegments,
							ulSessionId,
							ulCmdId,
							poconf,
							NULL // pceeval
							);

			GPOS_CHECK_ABORT;

			{
				CAutoTrace at(pmp);

				at.Os() << "Generated " <<  poconf->Pec()->UlCreatedSamples() <<" samples ... " << std::endl;

				// print ids of sampled plans
				CWStringDynamic *pstr = CDXLUtils::PstrSerializeSamplePlans(pmp, poconf->Pec(), true /*fIdent*/);
				at.Os() << pstr->Wsz();
				GPOS_DELETE(pstr);

				// print fitted cost distribution
				at.Os() << "Cost Distribution: " << std::endl;
				const ULONG ulSize = poconf->Pec()->UlCostDistrSize();
				for (ULONG ul = 0; ul < ulSize; ul++)
				{
					at.Os() << poconf->Pec()->DCostDistrX(ul) << "\t" << poconf->Pec()->DCostDistrY(ul) << std::endl;
				}

				// print serialized cost distribution
				pstr = CDXLUtils::PstrSerializeCostDistr(pmp, poconf->Pec(), true /*fIdent*/);

				at.Os() << pstr->Wsz();
				GPOS_DELETE(pstr);

			}

			// cleanup
			GPOS_DELETE(pdxlmd);
			pdxlnPlan->Release();

		} // end of MDAccessor scope


		// cleanup
		poconf->Release();
		pdrgpmdp->Release();


		(*pulTestCounter)++;
	}

	*pulTestCounter = 0;
	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresCheckPlans
//
//	@doc:
//		Check all enumerated plans using given PlanChecker function
//
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresCheckPlans
	(
	const CHAR *rgszFileNames[],
	ULONG ulTests,
	ULONG *pulTestCounter,
	ULONG ulSessionId,
	ULONG ulCmdId,
	FnPlanChecker *pfpc
	)
{
	GPOS_ASSERT(NULL != pfpc);

	GPOS_RESULT eres = GPOS_OK;

	for (ULONG ul = *pulTestCounter; ul < ulTests; ul++)
	{
		// each test uses a new memory pool to keep total memory consumption low
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();

		// reset metadata cache
		CMDCache::Reset();

		CAutoTraceFlag atf1(EopttraceEnumeratePlans, true);

		// load dump file
		CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, rgszFileNames[ul]);
		GPOS_CHECK_ABORT;

		// set up MD providers
		CMDProviderMemory *pmdp = GPOS_NEW(pmp) CMDProviderMemory(pmp, rgszFileNames[ul]);
		GPOS_CHECK_ABORT;

		const DrgPsysid *pdrgpsysid = pdxlmd->Pdrgpsysid();
		DrgPmdp *pdrgpmdp = GPOS_NEW(pmp) DrgPmdp(pmp);
		pdrgpmdp->Append(pmdp);

		for (ULONG ulSys = 1; ulSys < pdrgpsysid->UlLength(); ulSys++)
		{
			pmdp->AddRef();
			pdrgpmdp->Append(pmdp);
		}

		COptimizerConfig *poconf = pdxlmd->Poconf();

		if (NULL == poconf)
		{
			poconf = GPOS_NEW(pmp) COptimizerConfig
								(
								GPOS_NEW(pmp) CEnumeratorConfig(pmp, 0 /*ullPlanId*/, 1000 /*ullSamples*/),
								CStatisticsConfig::PstatsconfDefault(pmp),
								CCTEConfig::PcteconfDefault(pmp),
								ICostModel::PcmDefault(pmp),
								CHint::PhintDefault(pmp)
								);
		}
		else
		{
			poconf->AddRef();
		}

		ULONG ulSegments = UlSegments(poconf);

		// set plan checker
		poconf->Pec()->SetPlanChecker(pfpc);

		// allow sampler to throw invalid plan exception
		poconf->Pec()->SetSampleValidPlans(false /*fSampleValidPlans*/);

		{
			// scope for MD accessor
			CMDAccessor mda(pmp, CMDCache::Pcache(), pdrgpsysid, pdrgpmdp);

			CDXLNode *pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
							(
							pmp,
							&mda,
							pdxlmd,
							rgszFileNames[ul],
							ulSegments,
							ulSessionId,
							ulCmdId,
							poconf,
							NULL // pceeval
							);

			GPOS_CHECK_ABORT;

			// cleanup
			GPOS_DELETE(pdxlmd);
			pdxlnPlan->Release();

		} // end of MDAcessor scope

		poconf->Release();
		pdrgpmdp->Release();

		(*pulTestCounter)++;
	}

	*pulTestCounter = 0;
	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::UlSegments
//
//	@doc:
//		Return the number of segments, default return GPOPT_TEST_SEGMENTS
//
//---------------------------------------------------------------------------
ULONG
CTestUtils::UlSegments
	(
	COptimizerConfig *poconf
	)
{
	GPOS_ASSERT(NULL != poconf);
	ULONG ulSegments = GPOPT_TEST_SEGMENTS;
	if (NULL != poconf->Pcm())
	{
		ULONG ulSegs = poconf->Pcm()->UlHosts();
		if (ulSegments < ulSegs)
		{
			ulSegments = ulSegs;
		}
	}

	return ulSegments;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresCheckOptimizedPlan
//
//	@doc:
//		Check the optimized plan using given DXLPlanChecker function. Does
//		not take ownership of the given pdrgpcp. The cost model configured
//		in the minidumps must be the calibrated one.
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresCheckOptimizedPlan
	(
	const CHAR *rgszFileNames[],
	ULONG ulTests,
	ULONG *pulTestCounter,
	ULONG ulSessionId,
	ULONG ulCmdId,
	FnDXLPlanChecker *pfdpc,
	DrgPcp *pdrgpcp
	)
{
	GPOS_ASSERT(NULL != pfdpc);

	GPOS_RESULT eres = GPOS_OK;

	for (ULONG ul = *pulTestCounter; ul < ulTests; ul++)
	{
		// each test uses a new memory pool to keep total memory consumption low
		CAutoMemoryPool amp;
		IMemoryPool *pmp = amp.Pmp();

		// reset metadata cache
		CMDCache::Reset();

		CAutoTraceFlag atf1(EopttraceEnableSpacePruning, true /*fVal*/);

		// load dump file
		CDXLMinidump *pdxlmd = CMinidumperUtils::PdxlmdLoad(pmp, rgszFileNames[ul]);
		GPOS_CHECK_ABORT;

		// set up MD providers
		CMDProviderMemory *pmdp = GPOS_NEW(pmp) CMDProviderMemory(pmp, rgszFileNames[ul]);
		GPOS_CHECK_ABORT;

		const DrgPsysid *pdrgpsysid = pdxlmd->Pdrgpsysid();
		DrgPmdp *pdrgpmdp = GPOS_NEW(pmp) DrgPmdp(pmp);
		pdrgpmdp->Append(pmdp);

		for (ULONG ulSys = 1; ulSys < pdrgpsysid->UlLength(); ulSys++)
		{
			pmdp->AddRef();
			pdrgpmdp->Append(pmdp);
		}

		COptimizerConfig *poconf = pdxlmd->Poconf();
		GPOS_ASSERT(NULL != poconf);

		if (NULL != pdrgpcp)
		{
			poconf->Pcm()->SetParams(pdrgpcp);
		}
		poconf->AddRef();
		ULONG ulSegments = UlSegments(poconf);

		// allow sampler to throw invalid plan exception
		poconf->Pec()->SetSampleValidPlans(false /*fSampleValidPlans*/);

		{
			// scope for MD accessor
			CMDAccessor mda(pmp, CMDCache::Pcache(), pdrgpsysid, pdrgpmdp);

			CDXLNode *pdxlnPlan = CMinidumperUtils::PdxlnExecuteMinidump
							(
							pmp,
							&mda,
							pdxlmd,
							rgszFileNames[ul],
							ulSegments,
							ulSessionId,
							ulCmdId,
							poconf,
							NULL // pceeval
							);
			if (!pfdpc(pdxlnPlan))
			{
				eres = GPOS_FAILED;
				{
					CAutoTrace at(pmp);
					at.Os() << "Failed check for minidump " << rgszFileNames[ul] << std::endl;
					CWStringDynamic *pstrPlan = CDXLUtils::PstrSerializePlan
						(
						pmp,
						pdxlnPlan,
						0, // ullPlanId
						0, // ullPlanSpaceSize
						true, // fSerializeHeaderFooter
						true // fIndent
						);
					at.Os() << pstrPlan->Wsz() << std::endl;
					GPOS_DELETE(pstrPlan);
				}
			}

			GPOS_CHECK_ABORT;

			// cleanup
			GPOS_DELETE(pdxlmd);
			pdxlnPlan->Release();

		} // end of MDAcessor scope

		poconf->Release();
		pdrgpmdp->Release();

		(*pulTestCounter)++;
	}

	*pulTestCounter = 0;
	return eres;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PdatumGeneric
//
//	@doc:
//		Create a datum with a given type, encoded value and int value.
//
//---------------------------------------------------------------------------
IDatum *
CTestUtils::PdatumGeneric
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	IMDId *pmdidType,
	CWStringDynamic *pstrEncodedValue,
	LINT lValue
	)
{
	GPOS_ASSERT(NULL != pmda);

	GPOS_ASSERT(!pmdidType->FEquals(&CMDIdGPDB::m_mdidNumeric));
	const IMDType *pmdtype = pmda->Pmdtype(pmdidType);
	ULONG ulbaSize = 0;
	BYTE *pba = CDXLUtils::PByteArrayFromStr(pmp, pstrEncodedValue, &ulbaSize);

	CDXLDatumGeneric *pdxldatum = NULL;
	if (CMDTypeGenericGPDB::FTimeRelatedType(pmdidType))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumStatsDoubleMappable(pmp, pmdidType, pmdtype->FByValue() /*fConstByVal*/, false /*fConstNull*/, pba, ulbaSize, CDouble(lValue));
	}
	else if (pmdidType->FEquals(&CMDIdGPDB::m_mdidBPChar))
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumStatsLintMappable(pmp, pmdidType, pmdtype->FByValue() /*fConstByVal*/, false /*fConstNull*/, pba, ulbaSize, lValue);
	}
	else
	{
		pdxldatum = GPOS_NEW(pmp) CDXLDatumGeneric(pmp, pmdidType, pmdtype->FByValue() /*fConstByVal*/, false /*fConstNull*/, pba, ulbaSize);
	}

	IDatum *pdatum = pmdtype->Pdatum(pmp, pdxldatum);
	pdxldatum->Release();

	return pdatum;
}

//---------------------------------------------------------------------------
//      @function:
//              CConstraintTest::PciGenericInterval
//
//      @doc:
//              Create an interval for generic data types.
//              Does not take ownership of any argument.
//              Caller takes ownership of returned pointer.
//
//---------------------------------------------------------------------------
CConstraintInterval *
CTestUtils::PciGenericInterval
        (
        IMemoryPool *pmp,
        CMDAccessor *pmda,
        const CMDIdGPDB &mdidType,
        CColRef *pcr,
        CWStringDynamic *pstrLower,
        LINT lLower,
        CRange::ERangeInclusion eriLeft,
        CWStringDynamic *pstrUpper,
        LINT lUpper,
        CRange::ERangeInclusion eriRight
        )
{
	GPOS_ASSERT(NULL != pmda);

	IDatum *pdatumLower =
			CTestUtils::PdatumGeneric(pmp, pmda, GPOS_NEW(pmp) CMDIdGPDB(mdidType), pstrLower, lLower);
	IDatum *pdatumUpper =
			CTestUtils::PdatumGeneric(pmp, pmda, GPOS_NEW(pmp) CMDIdGPDB(mdidType), pstrUpper, lUpper);

	DrgPrng *pdrgprng = GPOS_NEW(pmp) DrgPrng(pmp);
	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(CMDIdGPDB::m_mdidDate);
	CRange *prange = GPOS_NEW(pmp) CRange
				(
				pmdid,
				COptCtxt::PoctxtFromTLS()->Pcomp(),
				pdatumLower,
				eriLeft,
				pdatumUpper,
				eriRight
				);
	pdrgprng->Append(prange);

	return GPOS_NEW(pmp) CConstraintInterval(pmp, pcr, pdrgprng, false /*fIsNull*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprScalarCmpIdentToConstant
//
//	@doc:
//		Helper for generating a scalar compare identifier to a constant
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprScalarCmpIdentToConstant
	(
	IMemoryPool *pmp,
	CExpression *pexpr
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexpr);

	CColRefSet *pcrs = CDrvdPropRelational::Pdprel(pexpr->PdpDerive())->PcrsOutput();
	CColRef *pcrAny =  pcrs->PcrAny();
	CExpression *pexprConst =  CUtils::PexprScalarConstInt4(pmp, 10 /* iVal */);

	return CUtils::PexprScalarEqCmp(pmp, pcrAny, pexprConst);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprExistsSubquery
//
//	@doc:
//		Helper for generating an exists subquery
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprExistsSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexprOuter);

	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);

	return CSubqueryTestUtils::PexprSubqueryExistential
			(
			pmp,
			COperator::EopScalarSubqueryExists,
			pexprOuter,
			pexprInner,
			false /* fCorrelated */
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexpSubqueryAll
//
//	@doc:
//		Helper for generating an ALL subquery
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexpSubqueryAll
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexprOuter);

	CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	const CColRef *pcrOuter = pcrsOuter->PcrAny();


	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrsInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	const CColRef *pcrInner = pcrsInner->PcrAny();

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarSubqueryAll
								(
								pmp,
								GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP),
								GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("=")),
								pcrInner
								),
					pexprInner,
					CUtils::PexprScalarIdent(pmp, pcrOuter)
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexpSubqueryAny
//
//	@doc:
//		Helper for generating an ANY subquery
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexpSubqueryAny
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexprOuter);

	CColRefSet *pcrsOuter = CDrvdPropRelational::Pdprel(pexprOuter->PdpDerive())->PcrsOutput();
	const CColRef *pcrOuter = pcrsOuter->PcrAny();


	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);
	CColRefSet *pcrsInner = CDrvdPropRelational::Pdprel(pexprInner->PdpDerive())->PcrsOutput();
	const CColRef *pcrInner = pcrsInner->PcrAny();

	return GPOS_NEW(pmp) CExpression
					(
					pmp,
					GPOS_NEW(pmp) CScalarSubqueryAny
								(
								pmp,
								GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP),
								GPOS_NEW(pmp) CWStringConst(GPOS_WSZ_LIT("=")),
								pcrInner
								),
								pexprInner,
					CUtils::PexprScalarIdent(pmp, pcrOuter)
					);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprNotExistsSubquery
//
//	@doc:
//		Helper for generating a not exists subquery
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprNotExistsSubquery
	(
	IMemoryPool *pmp,
	CExpression *pexprOuter
	)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pexprOuter);

	CExpression *pexprInner = CTestUtils::PexprLogicalGet(pmp);

	return CSubqueryTestUtils::PexprSubqueryExistential
			(
			pmp,
			COperator::EopScalarSubqueryNotExists,
			pexprOuter,
			pexprInner,
			false /* fCorrelated */
			);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::FHasOp
//
//	@doc:
//		 Recursively traverses the subtree rooted at the given expression, and
//  	 return the first subexpression it encounters that has the given id
//
//---------------------------------------------------------------------------
const CExpression *
CTestUtils::PexprFirst
	(
	const CExpression *pexpr,
	const COperator::EOperatorId eopid
	)
{
	GPOS_CHECK_STACK_SIZE;
	GPOS_ASSERT(NULL != pexpr);

	if (pexpr->Pop()->Eopid() == eopid)
	{
		return pexpr;
	}

	// recursively check children
	const ULONG ulArity = pexpr->UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		const CExpression *pexprFirst = PexprFirst((*pexpr)[ul], eopid);
		if (NULL != pexprFirst)
		{
			return pexprFirst;
		}

	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprAnd
//
//	@doc:
//		Generate a scalar AND expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprAnd
	(
	IMemoryPool *pmp,
	CExpression *pexprFst,
	CExpression *pexprSnd
	)
{
	GPOS_ASSERT(NULL != pexprFst);
	GPOS_ASSERT(NULL != pexprSnd);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprFst);
	pdrgpexpr->Append(pexprSnd);

	return CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopAnd, pdrgpexpr);
}


//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::PexprOr
//
//	@doc:
//		Generate a scalar OR expression
//
//---------------------------------------------------------------------------
CExpression *
CTestUtils::PexprOr
	(
	IMemoryPool *pmp,
	CExpression *pexprFst,
	CExpression *pexprSnd
	)
{
	GPOS_ASSERT(NULL != pexprFst);
	GPOS_ASSERT(NULL != pexprSnd);

	DrgPexpr *pdrgpexpr = GPOS_NEW(pmp) DrgPexpr(pmp);
	pdrgpexpr->Append(pexprFst);
	pdrgpexpr->Append(pexprSnd);

	return CUtils::PexprScalarBoolOp(pmp, CScalarBoolOp::EboolopOr, pdrgpexpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CTestUtils::EresUnittest_RunTests
//
//	@doc:
//		Run  Minidump-based tests in the given array of files
//
//---------------------------------------------------------------------------
GPOS_RESULT
CTestUtils::EresUnittest_RunTests
	(
	const CHAR **rgszFileNames,
	ULONG *pulTestCounter,
	ULONG ulTests
	)
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	BOOL fMatchPlans = false;
	BOOL fTestSpacePruning = false;
#if defined(GPOS_Darwin) || defined(GPOS_Linux)
	// restrict plan matching to OsX and Linux to avoid arithmetic operations differences
	// across systems
	fMatchPlans = true;
	fTestSpacePruning = true;
#endif // GPOS_Darwin || GPOS_Linux

	// enable (Redistribute, Broadcast) hash join plans
	CAutoTraceFlag atf1(EopttraceEnableRedistributeBroadcastHashJoin, true /*fVal*/);

	// enable plan enumeration only if we match plans
	CAutoTraceFlag atf2(EopttraceEnumeratePlans, fMatchPlans);

	// enable stats derivation for DPE
	CAutoTraceFlag atf3(EopttraceDeriveStatsForDPE, true /*fVal*/);

	// prefer MDQA
	CAutoTraceFlag atf5(EopttracePreferExpandedMDQAs, true);

	GPOS_RESULT eres =
			CTestUtils::EresRunMinidumps
						(
						pmp,
						rgszFileNames,
						ulTests,
						pulTestCounter,
						1, // ulSessionId
						1,  // ulCmdId
						fMatchPlans,
						fTestSpacePruning
						);

	return eres;
}


// EOF
