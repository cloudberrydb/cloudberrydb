//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CMDProviderCommTest.cpp
//
//	@doc:
//		Tests the file-based metadata provider with communicator.
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
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"
#include "gpos/net/CSocketConnectorUDS.h"
#include "gpos/net/CSocketListenerUDS.h"

#include "unittest/dxl/CMDProviderCommTest.h"
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

const CHAR *CMDProviderCommTest::szFileName = "../data/dxl/metadata/md.xml";
#define GPNAUCRATES_TEST_COMM_BUFFER_SIZE (128)

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommTest::EresUnittest
//
//	@doc:
//
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDProviderCommTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMDProviderCommTest::EresUnittest_MDProviderComm),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommTest::EresUnittest_MDProviderComm
//
//	@doc:
//		Test fetching non-exiting metadata objects from a file-based provider
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMDProviderCommTest::EresUnittest_MDProviderComm()
{
	Unittest_RunClientServer(PvUnittest_ServerBasic, PvUnittest_ClientBasic);
	Unittest_RunClientServer(PvUnittest_ServerBasic, PvUnittest_ClientTypeInfo);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommTest::Unittest_RunClientServer
//
//	@doc:
//		Run server and client for communicator-based MD provider
//
//---------------------------------------------------------------------------
void
CMDProviderCommTest::Unittest_RunClientServer
	(
	void *(*pfServer)(void *pv),
	void *(*pfClient)(void *pv)
	)
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CHAR szPath[GPNAUCRATES_TEST_COMM_BUFFER_SIZE];
	ioutils::ConstructTempFilePath(szPath, GPOS_ARRAY_SIZE(szPath));

	STestParams params;
	params.m_pmp = pmp;
	params.m_szPath = szPath;

	// scope for tasks
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[2];
		rgPtsk[0] = atp.PtskCreate(pfServer, &params);
		rgPtsk[1] = atp.PtskCreate(pfClient, &params);

		for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgPtsk); ul++)
		{
			atp.Schedule(rgPtsk[ul]);
		}

		for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgPtsk); ul++)
		{
			CTask *ptsk = NULL;
			atp.WaitAny(&ptsk);
		}
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommTest::PvUnittest_ServerBasic
//
//	@doc:
//		Server function on communicator-based MD test
//
//---------------------------------------------------------------------------
void *
CMDProviderCommTest::PvUnittest_ServerBasic
	(
	void *pv
	)
{
	CAutoTraceFlag atf(EtraceSimulateAbort, false);

	STestParams *pparams = static_cast<STestParams*>(pv);
	IMemoryPool *pmp = pparams->m_pmp;
	CSocketListenerUDS sl(pmp, pparams->m_szPath);
	sl.StartListener();
	CCommunicator comm(pmp, sl.PsocketNext());
	GPOS_CHECK_ABORT;

	// set up an MD proxy
	CMDProviderMemory *pmdpf = GPOS_NEW(pmp) CMDProviderMemory(pmp, szFileName);
	GPOS_CHECK_ABORT;

	CMDProviderCommProxy mdpcp(pmp, pmdpf);

	CCommMessage *pmsgRequest = comm.PmsgReceiveMsg();
	GPOS_ASSERT(CCommMessage::EcmtMDRequest == pmsgRequest->Ecmt());
	GPOS_CHECK_ABORT;

	// scope for auto trace
	{
		CAutoTrace at(pparams->m_pmp);
		at.Os() << "Request: " << pmsgRequest->Wsz();
	}

	CWStringBase *pstrResponse = mdpcp.PstrObject(pmsgRequest->Wsz());

	GPOS_DELETE_ARRAY(pmsgRequest->Wsz());
	GPOS_DELETE(pmsgRequest);

	// send response
	CCommMessage msgResponse(CCommMessage::EcmtMDResponse, pstrResponse->Wsz(), 0 /*ullUserData*/);
	
	GPOS_TRY
	{
		comm.SendMsg(&msgResponse);
		GPOS_CHECK_ABORT;
	}
	GPOS_CATCH_EX(ex)
	{
#ifdef GPOS_DEBUG
		if (CTestUtils::FFaultSimulation() &&
			((CException::ExmaSystem == ex.UlMajor() && CException::ExmiNetError == ex.UlMinor()) || 
			(CException::ExmaSystem == ex.UlMajor() && CException::ExmiAbort == ex.UlMinor())))
		{
			// simulating exceptions in client caused a network error in server
			GPOS_RESET_EX;
			return NULL;
		}
#endif // GPOS_DEBUG
		
		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	GPOS_DELETE(pstrResponse);
	pmdpf->Release();
	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommTest::PvUnittest_ClientBasic
//
//	@doc:
//		Client function on communicator-based MD test
//
//---------------------------------------------------------------------------
void *
CMDProviderCommTest::PvUnittest_ClientBasic
	(
	void *pv
	)
{
	CAutoTraceFlag atf(EtraceSimulateAbort, false);

	STestParams *pparams = static_cast<STestParams*>(pv);
	IMemoryPool *pmp = pparams->m_pmp;
	CSocketConnectorUDS sc(pmp, pparams->m_szPath);
	sc.Connect();

	CCommunicator comm(pmp, sc.Psocket());
	CMDProviderComm *pmdp = GPOS_NEW(pmp) CMDProviderComm(pmp, &comm);

	CMDIdGPDB *pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPOPT_MDCACHE_TEST_OID, 1 /* major version */, 1 /* minor version */);
	
	CWStringBase *pstrResponse = NULL;
		
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
		pmdp->AddRef();
		CAutoMDAccessor amda(pmp, pmdp, CTestUtils::m_sysidDefault, pcache);

		GPOS_TRY
		{
			pstrResponse = pmdp->PstrObject(pmp, amda.Pmda(), pmdid);
		}
		GPOS_CATCH_EX(ex)
		{
#ifdef GPOS_DEBUG
			if (CTestUtils::FFaultSimulation() &&
					((CException::ExmaSystem == ex.UlMajor() && CException::ExmiNetError == ex.UlMinor()) ||
							(CException::ExmaSystem == ex.UlMajor() && CException::ExmiAbort == ex.UlMinor())))
			{
				// simulating exceptions in server caused a network error in client
				GPOS_RESET_EX;
				return NULL;
			}
#endif // GPOS_DEBUG

			GPOS_RETHROW(ex);
		}
		GPOS_CATCH_END;
		// scope for auto trace
		{
			CAutoTrace at(pparams->m_pmp);
			at.Os() << "Response: " << pstrResponse->Wsz();
		}
	}
	// cleanup
	GPOS_DELETE(pstrResponse);
	pmdp->Release();
	pmdid->Release();

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommTest::PvUnittest_ClientTypeInfo
//
//	@doc:
//		Client function on communicator-based MD test
//
//---------------------------------------------------------------------------
void *
CMDProviderCommTest::PvUnittest_ClientTypeInfo
	(
	void *pv
	)
{
	CAutoTraceFlag atf(EtraceSimulateAbort, false);

	STestParams *pparams = static_cast<STestParams*>(pv);
	IMemoryPool *pmp = pparams->m_pmp;
	CSocketConnectorUDS sc(pmp, pparams->m_szPath);
	sc.Connect();
	CCommunicator comm(pmp, sc.Psocket());

	CMDProviderComm *pmdp = GPOS_NEW(pmp) CMDProviderComm(pmp, &comm);

	// request type info
	IMDId *pmdidBool = NULL;
	
	GPOS_TRY
	{
		pmdidBool = pmdp->Pmdid(pmp, CTestUtils::m_sysidDefault, IMDType::EtiBool);
	}
	GPOS_CATCH_EX(ex)
	{
#ifdef GPOS_DEBUG
		if (CTestUtils::FFaultSimulation() &&
			((CException::ExmaSystem == ex.UlMajor() && CException::ExmiNetError == ex.UlMinor()) || 
			(CException::ExmaSystem == ex.UlMajor() && CException::ExmiAbort == ex.UlMinor())))			
		{
			// simulating exceptions in server caused a network error in client
			GPOS_RESET_EX;
			return NULL;
		}
#endif // GPOS_DEBUG

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	// scope for auto trace
	{
		CAutoTrace at(pparams->m_pmp);
		at.Os() << "Response: " << pmdidBool->Wsz() << std::endl;
	}

	// cleanup
	pmdp->Release();
	pmdidBool->Release();

	return NULL;
}

// EOF
