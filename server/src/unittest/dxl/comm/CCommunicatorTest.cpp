//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCommunicatorTest.cpp
//
//	@doc:
//		Test for communication handler
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/error/CErrorHandlerStandard.h"
#include "gpos/io/ioutils.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/net/CAutoSocketProxy.h"
#include "gpos/net/CSocketConnectorUDS.h"
#include "gpos/net/CSocketListenerUDS.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"

#include "naucrates/exception.h"
#include "naucrates/comm/CLoggerComm.h"

#include "unittest/dxl/comm/CCommunicatorTest.h"

#define GPNAUCRATES_TEST_COMM_QUERY_REQ		GPOS_WSZ_LIT("This is a query request")
#define GPNAUCRATES_TEST_COMM_QUERY_RESP	GPOS_WSZ_LIT("This is a query response")
#define GPNAUCRATES_TEST_COMM_BUFFER_SIZE	(128)
#define GPNAUCRATES_TEST_COMM_RETRY_US		(1000)

using namespace gpos;
using namespace gpnaucrates;


//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::EresUnittest
//
//	@doc:
//		Unittest for scheduler
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCommunicatorTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(EresUnittest_Basic),
		GPOS_UNITTEST_FUNC_THROW(EresUnittest_Error, gpdxl::ExmaComm, gpdxl::ExmiCommPropagateError),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::EresUnittest_Basic
//
//	@doc:
//		Basic communication test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCommunicatorTest::EresUnittest_Basic()
{
	Unittest_RunClientServer(PvUnittest_ServerBasic, PvUnittest_ClientBasic);

	return GPOS_OK;
}

//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::EresUnittest_Error
//
//	@doc:
//		Exception propagation test
//
//---------------------------------------------------------------------------
GPOS_RESULT
CCommunicatorTest::EresUnittest_Error()
{
	Unittest_RunClientServer(PvUnittest_ServerError, PvUnittest_ClientError);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::Unittest_RunClientServer
//
//	@doc:
//		Basic communication test
//
//---------------------------------------------------------------------------
void
CCommunicatorTest::Unittest_RunClientServer
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
//		CCommunicatorTest::PvUnittest_ServerBasic
//
//	@doc:
//		Server function on basic test
//
//---------------------------------------------------------------------------
void *
CCommunicatorTest::PvUnittest_ServerBasic
	(
	void *pv
	)
{
	STestParams *pparams = static_cast<STestParams*>(pv);
	CSocketListenerUDS sl(pparams->m_pmp, pparams->m_szPath);
	sl.StartListener();
	CCommunicator comm(pparams->m_pmp, sl.PsocketNext());

	CCommMessage *pmsgRequest = comm.PmsgReceiveMsg();

	// scope for auto trace
	{
		CAutoTrace at(pparams->m_pmp);
		at.Os() << pmsgRequest->Wsz();
	}

	GPOS_ASSERT(CCommMessage::EcmtOptRequest == pmsgRequest->Ecmt());
	GPOS_ASSERT
		(
		0 == clib::IWcsNCmp
				(
				GPNAUCRATES_TEST_COMM_QUERY_REQ,
				pmsgRequest->Wsz(),
				GPOS_WSZ_LENGTH(GPNAUCRATES_TEST_COMM_QUERY_REQ)
				)
		);

	GPOS_DELETE_ARRAY(pmsgRequest->Wsz());
	GPOS_DELETE(pmsgRequest);

	// send response
	const WCHAR *wszReq = GPNAUCRATES_TEST_COMM_QUERY_RESP;
	CCommMessage msgResponse(CCommMessage::EcmtOptResponse, wszReq, 0 /*ullUserData*/);
	comm.SendMsg(&msgResponse);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::PvUnittest_ClientBasic
//
//	@doc:
//		Client function on basic test
//
//---------------------------------------------------------------------------
void *
CCommunicatorTest::PvUnittest_ClientBasic
	(
	void *pv
	)
{
	STestParams *pparams = static_cast<STestParams*>(pv);
	CSocketConnectorUDS sc(pparams->m_pmp, pparams->m_szPath);
	sc.Connect();
	CCommunicator comm(pparams->m_pmp, sc.Psocket());

	// send query request - get response
	const WCHAR *wszReq = GPNAUCRATES_TEST_COMM_QUERY_REQ;
	CCommMessage msgRequest(CCommMessage::EcmtOptRequest, wszReq, 0 /*ullUserData*/);
	CCommMessage *pmsgResponse = comm.PmsgSendRequest(&msgRequest);

	// scope for auto trace
	{
		CAutoTrace at(pparams->m_pmp);
		at.Os() << pmsgResponse->Wsz();
	}

	GPOS_ASSERT(CCommMessage::EcmtOptResponse == pmsgResponse->Ecmt());
	GPOS_ASSERT
		(
		0 == clib::IWcsNCmp
				(
				GPNAUCRATES_TEST_COMM_QUERY_RESP,
				pmsgResponse->Wsz(),
				GPOS_WSZ_LENGTH(GPNAUCRATES_TEST_COMM_QUERY_RESP)
				)
		);

	GPOS_DELETE_ARRAY(pmsgResponse->Wsz());
	GPOS_DELETE(pmsgResponse);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::PvUnittest_ServerError
//
//	@doc:
//		Server function on error test
//
//---------------------------------------------------------------------------
void *
CCommunicatorTest::PvUnittest_ServerError
	(
	void * pv
	)
{
	STestParams *pparams = static_cast<STestParams*>(pv);
	CSocketListenerUDS sl(pparams->m_pmp, pparams->m_szPath);
	sl.StartListener();
	CCommunicator comm(pparams->m_pmp, sl.PsocketNext());

	// receive request
	CCommMessage *pmsgRequest = comm.PmsgReceiveMsg();
	GPOS_ASSERT(CCommMessage::EcmtOptRequest == pmsgRequest->Ecmt());
	GPOS_DELETE_ARRAY(pmsgRequest->Wsz());
	GPOS_DELETE(pmsgRequest);

	CLoggerComm *plogger = GPOS_NEW(pparams->m_pmp) CLoggerComm(&comm);
	ILogger *ploggerBack = ITask::PtskSelf()->Ptskctxt()->PlogErr();
	ITask::PtskSelf()->Ptskctxt()->SetLogErr(plogger);

	// simulate exception during request processing
	CErrorHandlerStandard errhdl;
	GPOS_TRY_HDL(&errhdl)
	{
		GPOS_ASSERT(!"Test exception propagation using communicator");
	}
	GPOS_CATCH_EX(ex)
	{
		GPOS_RESET_EX;
	}
	GPOS_CATCH_END;

	// restore logger
	ITask::PtskSelf()->Ptskctxt()->SetLogErr(ploggerBack);
	GPOS_DELETE(plogger);

	// wait until client throws
	while (true)
	{
		GPOS_CHECK_ABORT;

		clib::USleep(GPNAUCRATES_TEST_COMM_RETRY_US);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CCommunicatorTest::PvUnittest_ClientError
//
//	@doc:
//		Client function on error test
//
//---------------------------------------------------------------------------
void *
CCommunicatorTest::PvUnittest_ClientError
	(
	void * pv
	)
{
	STestParams *pparams = static_cast<STestParams*>(pv);
	CSocketConnectorUDS sc(pparams->m_pmp, pparams->m_szPath);
	sc.Connect();
	CCommunicator comm(pparams->m_pmp, sc.Psocket());

	// send query request - get response
	const WCHAR *wszReq = GPNAUCRATES_TEST_COMM_QUERY_REQ;
	CCommMessage msgRequest(CCommMessage::EcmtOptRequest, wszReq, 0 /*ullUserData*/);
	CCommMessage *pmsgResponse = comm.PmsgSendRequest(&msgRequest);

	// scope for auto trace
	{
		CAutoTrace at(pparams->m_pmp);
		at.Os() << pmsgResponse->Wsz();
	}

	GPOS_ASSERT(CCommMessage::EcmtLog == pmsgResponse->Ecmt());
	GPOS_ASSERT(CException::ExsevError == pmsgResponse->UllInfo());
	GPOS_RAISE(gpdxl::ExmaComm, gpdxl::ExmiCommPropagateError, pmsgResponse->Wsz());

	return NULL;
}


// EOF
