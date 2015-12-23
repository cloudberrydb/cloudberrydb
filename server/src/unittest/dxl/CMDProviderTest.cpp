//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDProviderTest.cpp
//
//	@doc:
//		Tests the file-based metadata provider.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/io/ioutils.h"
#include "gpos/io/COstreamString.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"
#include "gpos/net/CSocketConnectorUDS.h"
#include "gpos/net/CSocketListenerUDS.h"

#include "unittest/gpopt/mdcache/CMDProviderTest.h"
#include "unittest/gpopt/CTestUtils.h"

#include "naucrates/md/CMDProviderComm.h"
#include "naucrates/md/CMDProviderCommProxy.h"
#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdColStats.h"

#include "naucrates/comm/CCommunicator.h"

#include "naucrates/exception.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;

const CHAR *CMDProviderTest::szFileName = "../data/dxl/metadata/md.xml";
#define GPNAUCRATES_TEST_COMM_BUFFER_SIZE (128)

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderTest::EresUnittest
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDProviderTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMDProviderTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CMDProviderTest::EresUnittest_Stats),
		GPOS_UNITTEST_FUNC_THROW
			(
			CMDProviderTest::EresUnittest_Negative,
			gpdxl::ExmaMD,
			gpdxl::ExmiMDCacheEntryNotFound
			),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderTest::EresUnittest_Basic
//
//	@doc:
//		Test fetching existing metadata objects from a file-based provider
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDProviderTest::EresUnittest_Basic()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	// test lookup with a file-based provider
	CMDProviderMemory *pmdpFile = GPOS_NEW(pmp) CMDProviderMemory(pmp, szFileName);
	pmdpFile->AddRef();
	
	TestMDLookup(pmp, pmdpFile);
	
	pmdpFile->Release();
	
	// test lookup with a memory-based provider
	CHAR *szDXL = CDXLUtils::SzRead(pmp, szFileName);

	DrgPimdobj *pdrgpmdobj = CDXLUtils::PdrgpmdobjParseDXL(pmp, szDXL, NULL /*szXSDPath*/);
	
	CMDProviderMemory *pmdpMemory = GPOS_NEW(pmp) CMDProviderMemory(pmp, pdrgpmdobj);
	pmdpMemory->AddRef();
	TestMDLookup(pmp, pmdpMemory);

	GPOS_DELETE_ARRAY(szDXL);
	pdrgpmdobj->Release();
	pmdpMemory->Release();
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderTest::TestMDLookup
//
//	@doc:
//		Test looking up objects using given MD provider
//
//---------------------------------------------------------------------------
void
CMDProviderTest::TestMDLookup
	(
	IMemoryPool *pmp,
	IMDProvider *pmdp
	)
{
	CAutoMDAccessor amda(pmp, pmdp, CTestUtils::m_sysidDefault, CMDCache::Pcache());

	// lookup different objects
	CMDIdGPDB *pmdid1 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1 /* major version */, 1 /* minor version */);
	CMDIdGPDB *pmdid2 = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 12 /* version */, 1 /* minor version */);

	CWStringBase *pstrMDObject1 = pmdp->PstrObject(pmp, amda.Pmda(), pmdid1);
	CWStringBase *pstrMDObject2 = pmdp->PstrObject(pmp, amda.Pmda(), pmdid2);

	GPOS_ASSERT(NULL != pstrMDObject1 && NULL != pstrMDObject2);

	IMDCacheObject *pimdobj1 = CDXLUtils::PimdobjParseDXL(pmp, pstrMDObject1, NULL);

	IMDCacheObject *pimdobj2 = CDXLUtils::PimdobjParseDXL(pmp, pstrMDObject2, NULL);

	GPOS_ASSERT(NULL != pimdobj1 && pmdid1->FEquals(pimdobj1->Pmdid()));
	GPOS_ASSERT(NULL != pimdobj2 && pmdid2->FEquals(pimdobj2->Pmdid()));

	// cleanup
	pmdid1->Release();
	pmdid2->Release();
	GPOS_DELETE(pstrMDObject1);
	GPOS_DELETE(pstrMDObject2);
	pimdobj1->Release();
	pimdobj2->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderTest::EresUnittest_Stats
//
//	@doc:
//		Test fetching existing stats objects from a file-based provider
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDProviderTest::EresUnittest_Stats()
{
	// create memory pool
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();
	
	CMDProviderMemory *pmdpFile = GPOS_NEW(pmp) CMDProviderMemory(pmp, szFileName);

	{
		pmdpFile->AddRef();
		CAutoMDAccessor amda(pmp, pmdpFile, CTestUtils::m_sysidDefault, CMDCache::Pcache());

		// lookup different objects
		CMDIdRelStats *pmdidRelStats = GPOS_NEW(pmp) CMDIdRelStats(GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1));

		CWStringBase *pstrRelStats = pmdpFile->PstrObject(pmp, amda.Pmda(), pmdidRelStats);
		GPOS_ASSERT(NULL != pstrRelStats);
		IMDCacheObject *pmdobjRelStats = CDXLUtils::PimdobjParseDXL(pmp, pstrRelStats, NULL);
		GPOS_ASSERT(NULL != pmdobjRelStats);

		CMDIdColStats *pmdidColStats =
				GPOS_NEW(pmp) CMDIdColStats(GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1, 1), 1 /* ulAttno */);

		CWStringBase *pstrColStats = pmdpFile->PstrObject(pmp, amda.Pmda(), pmdidColStats);
		GPOS_ASSERT(NULL != pstrColStats);
		IMDCacheObject *pmdobjColStats = CDXLUtils::PimdobjParseDXL(pmp, pstrColStats, NULL);
		GPOS_ASSERT(NULL != pmdobjColStats);

		// cleanup
		pmdidRelStats->Release();
		pmdidColStats->Release();
		GPOS_DELETE(pstrRelStats);
		GPOS_DELETE(pstrColStats);
		pmdobjRelStats->Release();
		pmdobjColStats->Release();
	}

	pmdpFile->Release();
	
	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderTest::EresUnittest_Negative
//
//	@doc:
//		Test fetching non-exiting metadata objects from a file-based provider
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDProviderTest::EresUnittest_Negative()
{
	CAutoMemoryPool amp(CAutoMemoryPool::ElcNone);
	IMemoryPool *pmp = amp.Pmp();
	
	CMDProviderMemory *pmdpFile = GPOS_NEW(pmp) CMDProviderMemory(pmp, szFileName);
	pmdpFile->AddRef();
	
	// we need to use an auto pointer for the cache here to ensure
	// deleting memory of cached objects when we throw
	CAutoP<CMDAccessor::MDCache> apcache;
	apcache = CCacheFactory::PCacheCreate<gpopt::IMDCacheObject*, gpopt::CMDKey*>
				(
				true, // fUnique
				0 /* unlimited cache quota */,
				CMDKey::UlHashMDKey,
				CMDKey::FEqualMDKey
				);

	CMDAccessor::MDCache *pcache = apcache.Pt();

	{
		CAutoMDAccessor amda(pmp, pmdpFile, CTestUtils::m_sysidDefault, pcache);

		// lookup a non-existing objects
		CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 15 /* major version */, 1 /* minor version */);

		// call should result in an exception
		(void) pmdpFile->PstrObject(pmp, amda.Pmda(), pmdid);
	}
	
	return GPOS_FAILED;
}

// EOF

