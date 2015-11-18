//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSocketTest.cpp
//
//	@doc:
//		Tests for socket operations
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/error/CAutoTrace.h"
#include "gpos/io/ioutils.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/net/CAutoSocketProxy.h"
#include "gpos/net/CSocketConnectorUDS.h"
#include "gpos/net/CSocketListenerUDS.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/net/CSocketTest.h"

using namespace gpos;
using namespace gpos::ioutils;

#define GPOS_TEST_NET_CLIENT_MSG	"This is a client request"
#define GPOS_TEST_NET_SERVER_MSG	"This is a server response"
#define GPOS_TEST_NET_BUFFER_SIZE	(1024)

//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::EresUnittest
//
//	@doc:
//		Driver for unittests
//---------------------------------------------------------------------------
GPOS_RESULT
CSocketTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CSocketTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CSocketTest::EresUnittest_Concurrency),
		GPOS_UNITTEST_FUNC(CSocketTest::EresUnittest_Stress),
		GPOS_UNITTEST_FUNC_THROW
			(
			EresUnittest_InvalidListener,
			CException::ExmaSystem,
			CException::ExmiNetError
			),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::EresUnittest_Basic
//
//	@doc:
//		Test ability to send a message and receive a response
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSocketTest::EresUnittest_Basic()
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *pmp = amp.Pmp();

	CHAR szPath[GPOS_TEST_NET_BUFFER_SIZE];
	ConstructTempFilePath(szPath, GPOS_ARRAY_SIZE(szPath));

	SSocketTestParams initUds;
	initUds.m_pmp = pmp;
	initUds.m_szPath = szPath;
	initUds.m_ulId = 0;
	initUds.m_ulClients = 1;
	initUds.m_ulIterations = 1;

	GPOS_TRY
	{
		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[2];
		rgPtsk[0] = atp.PtskCreate(PvUnittest_SendBasic, &initUds);
		rgPtsk[1] = atp.PtskCreate(PvUnittest_RecvBasic, &initUds);

		// start sender first - should block until receiver is listening
		atp.Schedule(rgPtsk[0]);

		// wait for a while
		clib::USleep(10000);

		// start receiver
		atp.Schedule(rgPtsk[1]);

		for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgPtsk); ul++)
		{
			CTask *ptsk = NULL;
			atp.WaitAny(&ptsk);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		CheckException(ex);
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::EresUnittest_Concurrency
//
//	@doc:
//		Test ability to serve many clients concurrently
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSocketTest::EresUnittest_Concurrency()
{
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *pmp = amp.Pmp();

	const ULONG ulClients = 4;

	CHAR szPath[GPOS_TEST_NET_BUFFER_SIZE];
	ConstructTempFilePath(szPath, GPOS_ARRAY_SIZE(szPath));

	GPOS_TRY
	{

		SSocketTestParams initUds[ulClients + 1];
		CTask *rgPtsk[ulClients + 1];
		CAutoTaskProxy atp(pmp, pwpm);

		// schedule clients
		for (ULONG ul = 0; ul < ulClients; ul++)
		{
			initUds[ul].m_pmp = pmp;
			initUds[ul].m_szPath = szPath;
			initUds[ul].m_ulId = ul;
			initUds[ul].m_ulClients = ulClients;
			initUds[ul].m_ulIterations = 1;

			rgPtsk[ul] = atp.PtskCreate(PvUnittest_SendConcurrent, &initUds[ul]);

			// start client - should block until receiver is listening
			atp.Schedule(rgPtsk[ul]);
		}

		// schedule server
		initUds[ulClients].m_pmp = pmp;
		initUds[ulClients].m_szPath = szPath;
		initUds[ulClients].m_ulId = ulClients;
		initUds[ulClients].m_ulClients = ulClients;
		initUds[ulClients].m_ulIterations = 1;

		rgPtsk[ulClients] = atp.PtskCreate(PvUnittest_RecvConcurrent, &initUds);
		atp.Schedule(rgPtsk[ulClients]);

		for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgPtsk); ul++)
		{
			CTask *ptsk = NULL;
			atp.WaitAny(&ptsk);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		CheckException(ex);
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::EresUnittest_Stress
//
//	@doc:
//		Test ability to communicate under heavy load
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSocketTest::EresUnittest_Stress()
{
#ifdef GPOS_DEBUG
	// this test uses too many tasks;
	// it cannot pass time slice test unless time is measured per worker;
	if (IWorker::m_fEnforceTimeSlices)
	{
		return GPOS_OK;
	}
#endif // GPOS_DEBUG

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();
	CAutoMemoryPool amp(CAutoMemoryPool::ElcStrict);
	IMemoryPool *pmp = amp.Pmp();

	ULONG ulWorkersMax = pwpm->UlWorkersMax();

	GPOS_TRY
	{
		const ULONG ulClients = 32;
		const ULONG ulIterations = 128;

		pwpm->SetWorkersMax(2 *ulClients /*ulWorkers*/);

		CHAR szPath[GPOS_TEST_NET_BUFFER_SIZE];
		ConstructTempFilePath(szPath, GPOS_ARRAY_SIZE(szPath));

		// scope for tasks
		{
			SSocketTestParams initUds[ulClients + 1];
			CTask *rgPtsk[ulClients + 1];
			CAutoTaskProxy atp(pmp, pwpm);

			// schedule clients
			for (ULONG ul = 0; ul < ulClients; ul++)
			{
				initUds[ul].m_pmp = pmp;
				initUds[ul].m_szPath = szPath;
				initUds[ul].m_ulId = ul;
				initUds[ul].m_ulClients = ulClients;
				initUds[ul].m_ulIterations = ulIterations;

				rgPtsk[ul] = atp.PtskCreate(PvUnittest_SendStress, &initUds[ul]);

				// start client - should block until receiver is listening
				atp.Schedule(rgPtsk[ul]);
			}

			// schedule server
			initUds[ulClients].m_pmp = pmp;
			initUds[ulClients].m_szPath = szPath;
			initUds[ulClients].m_ulId = ulClients;
			initUds[ulClients].m_ulClients = ulClients;
			initUds[ulClients].m_ulIterations = ulIterations;

			rgPtsk[ulClients] = atp.PtskCreate(PvUnittest_RecvStress, &initUds);
			atp.Schedule(rgPtsk[ulClients]);

			for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgPtsk); ul++)
			{
				CTask *ptsk = NULL;
				atp.WaitAny(&ptsk);
			}
		}
	}
	GPOS_CATCH_EX(ex)
	{
		pwpm->SetWorkersMax(ulWorkersMax);

		CheckException(ex);
	}
	GPOS_CATCH_END;

	pwpm->SetWorkersMax(ulWorkersMax);

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::EresUnittest_InvalidListener
//
//	@doc:
//		Test handling of creating an invalid connector
//
//---------------------------------------------------------------------------
GPOS_RESULT
CSocketTest::EresUnittest_InvalidListener()
{
	CAutoMemoryPool amp;

	CHAR szPath[GPOS_TEST_NET_BUFFER_SIZE];
	ConstructTempFilePath(szPath, GPOS_ARRAY_SIZE(szPath));

	CSocketListenerUDS sl1(amp.Pmp(), szPath);
	sl1.StartListener();

	// attempt to bind second listener to same file - should throw
	CSocketListenerUDS sl2(amp.Pmp(), szPath);
	sl2.StartListener();

	return GPOS_FAILED;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_SendBasic
//
//	@doc:
//		Send a message and receive expected response
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_SendBasic
	(
	void *pv
	)
{
	SSocketTestParams *pinitUds = static_cast<SSocketTestParams*>(pv);
	CSocketConnectorUDS sc(pinitUds->m_pmp, pinitUds->m_szPath);
	sc.Connect();
	CSocket *psocket = sc.Psocket();

	const CHAR *szSend = GPOS_TEST_NET_CLIENT_MSG;
	psocket->Send((void *) szSend, GPOS_SZ_LENGTH(szSend) + 1);

	CHAR szRecv[GPOS_TEST_NET_BUFFER_SIZE];
#ifdef GPOS_DEBUG
	ULONG ulRecvLen =
#endif // GPOS_DEBUG
	psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

	// scope for logging
	{
		CAutoTrace at(pinitUds->m_pmp);
		at.Os() << szRecv;
	}

	GPOS_ASSERT(ulRecvLen == GPOS_SZ_LENGTH(GPOS_TEST_NET_SERVER_MSG) + 1);
	GPOS_ASSERT(0 == clib::IStrCmp(GPOS_TEST_NET_SERVER_MSG, szRecv));

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_RecvBasic
//
//	@doc:
//		Receive expected message and send response
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_RecvBasic
	(
	void *pv
	)
{
	SSocketTestParams *pinitUds = static_cast<SSocketTestParams*>(pv);
	CSocketListenerUDS sl(pinitUds->m_pmp, pinitUds->m_szPath);
	sl.StartListener();
	CSocket *psocket = sl.PsocketNext();

	GPOS_ASSERT(psocket->FCheck());

	CHAR szRecv[GPOS_TEST_NET_BUFFER_SIZE];
#ifdef GPOS_DEBUG
	ULONG ulRecvLen =
#endif // GPOS_DEBUG
	psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

	// scope for logging
	{
		CAutoTrace at(pinitUds->m_pmp);
		at.Os() << szRecv;
	}

	GPOS_ASSERT(ulRecvLen == GPOS_SZ_LENGTH(GPOS_TEST_NET_CLIENT_MSG) + 1);
	GPOS_ASSERT(0 == clib::IStrCmp(GPOS_TEST_NET_CLIENT_MSG, szRecv));

	const CHAR *szSend = GPOS_TEST_NET_SERVER_MSG;
	psocket->Send((void*) szSend, GPOS_SZ_LENGTH(szSend) + 1);

	// block until client closes connection
	while (psocket->FCheck())
	{
		clib::USleep(1000);
	}

	sl.Release(psocket);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_SendConcurrent
//
//	@doc:
//		Send a message with client ID and receive expected response
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_SendConcurrent
	(
	void *pv
	)
{
	SSocketTestParams *pinitUds = static_cast<SSocketTestParams*>(pv);
	CSocketConnectorUDS sc(pinitUds->m_pmp, pinitUds->m_szPath);
	sc.Connect();
	CSocket *psocket = sc.Psocket();

	// send client message
	const CHAR *szSend = GPOS_TEST_NET_CLIENT_MSG;
	psocket->Send((void *) szSend, GPOS_SZ_LENGTH(szSend) + 1);

	CHAR szRecv[GPOS_TEST_NET_BUFFER_SIZE];
#ifdef GPOS_DEBUG
	ULONG ulRecvLen =
#endif // GPOS_DEBUG
	psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

	GPOS_ASSERT(ulRecvLen == GPOS_SZ_LENGTH(GPOS_TEST_NET_SERVER_MSG) + 1);
	GPOS_ASSERT(0 == clib::IStrCmp(GPOS_TEST_NET_SERVER_MSG, szRecv));

	// scope for logging
	{
		CAutoTrace at(pinitUds->m_pmp);
		at.Os() << "Send request " << pinitUds->m_ulId;
	}

	// send client ID
	psocket->Send((void *) &pinitUds->m_ulId, GPOS_SIZEOF(pinitUds->m_ulId));
#ifdef GPOS_DEBUG
	ulRecvLen =
#endif // GPOS_DEBUG
	psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

	GPOS_ASSERT(ulRecvLen == GPOS_SIZEOF(pinitUds->m_ulId));
	GPOS_ASSERT((*(ULONG *) szRecv) == pinitUds->m_ulId);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_RecvConcurrent
//
//	@doc:
//		Receive messages from multiple clients and send response
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_RecvConcurrent
	(
	void *pv
	)
{
	SSocketTestParams *pinitUds = static_cast<SSocketTestParams*>(pv);
	CSocketListenerUDS sl(pinitUds->m_pmp, pinitUds->m_szPath);
	sl.StartListener();

	for (ULONG ul = 0; ul < pinitUds->m_ulClients; ul++)
	{
		CAutoSocketProxy asp(sl.PsocketNext());
		CSocket *psocket = asp.Psocket();

		// receive client message
		CHAR szRecv[GPOS_TEST_NET_BUFFER_SIZE];
		ULONG ulRecvLen = psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

		GPOS_ASSERT(ulRecvLen == GPOS_SZ_LENGTH(GPOS_TEST_NET_CLIENT_MSG) + 1);
		GPOS_ASSERT(0 == clib::IStrCmp(GPOS_TEST_NET_CLIENT_MSG, szRecv));

		const CHAR *szSend = GPOS_TEST_NET_SERVER_MSG;
		psocket->Send((void*) szSend, GPOS_SZ_LENGTH(szSend) + 1);

		// receive client ID
		ulRecvLen = psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));
		GPOS_ASSERT(ulRecvLen == GPOS_SIZEOF(pinitUds->m_ulId));

		// scope for logging
		{
			CAutoTrace at(pinitUds->m_pmp);
			ULONG_PTR *ulp = (ULONG_PTR*) szRecv;
			at.Os() << "Received request " << (ULONG)*ulp;
		}

		psocket->Send((void*) szRecv, ulRecvLen);

		// block until client closes connection
		while (psocket->FCheck())
		{
			clib::USleep(1000);
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_SendStress
//
//	@doc:
//		Send multiple messages and receive expected response
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_SendStress
	(
	void *pv
	)
{
	SSocketTestParams *pinitUds = static_cast<SSocketTestParams*>(pv);
	CSocketConnectorUDS sc(pinitUds->m_pmp, pinitUds->m_szPath);
	sc.Connect();
	CSocket *psocket = sc.Psocket();

	for (ULONG ul = 0; ul < pinitUds->m_ulIterations; ul++)
	{
		// send client message
		const CHAR *szSend = GPOS_TEST_NET_CLIENT_MSG;
		psocket->Send((void *) szSend, GPOS_SZ_LENGTH(szSend) + 1);

		CHAR szRecv[GPOS_TEST_NET_BUFFER_SIZE];
#ifdef GPOS_DEBUG
		ULONG ulRecvLen =
#endif // GPOS_DEBUG
		psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

		GPOS_ASSERT(ulRecvLen == GPOS_SZ_LENGTH(GPOS_TEST_NET_SERVER_MSG) + 1);
		GPOS_ASSERT(0 == clib::IStrCmp(GPOS_TEST_NET_SERVER_MSG, szRecv));

		// send client ID
		psocket->Send((void *) &pinitUds->m_ulId, GPOS_SIZEOF(pinitUds->m_ulId));
#ifdef GPOS_DEBUG
		ulRecvLen =
#endif // GPOS_DEBUG
		psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

		GPOS_ASSERT(ulRecvLen == GPOS_SIZEOF(pinitUds->m_ulId));
		GPOS_ASSERT((*(ULONG *) szRecv) == pinitUds->m_ulId);

		// send remaining iteration count
		ULONG ulRemaining = pinitUds->m_ulIterations - ul - 1;
		psocket->Send((void *) &ulRemaining, GPOS_SIZEOF(ulRemaining));
#ifdef GPOS_DEBUG
		ulRecvLen =
#endif // GPOS_DEBUG
		psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

		GPOS_ASSERT(ulRecvLen == GPOS_SIZEOF(ulRemaining));
		GPOS_ASSERT((*(ULONG *) szRecv) == ulRemaining);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_RecvStress
//
//	@doc:
//		Receive messages from multiple clients and assign them to tasks
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_RecvStress
	(
	void *pv
	)
{
	SSocketTestParams *pinitUds = static_cast<SSocketTestParams*>(pv);
	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	CSocketListenerUDS sl(pinitUds->m_pmp, pinitUds->m_szPath);
	sl.StartListener();
	CTask *ptsk = NULL;

	// scope for ATP
	{
		CAutoTaskProxy atp(pinitUds->m_pmp, pwpm);

		for (ULONG ul = 0; ul < pinitUds->m_ulClients; ul++)
		{
			CSocket *psocket = sl.PsocketNext();

			// create new task to handle communication
			ptsk = atp.PtskCreate(PvUnittest_RecvTask, psocket);
			atp.Schedule(ptsk);

			// release completed tasks
			while (0 != atp.UlTasks() &&
				   GPOS_OK == atp.EresTimedWaitAny(&ptsk, 0 /*ulTimeoutMs*/))
			{
				atp.Destroy(ptsk);
			}
		}

		// wait until all tasks complete
		while (0 != atp.UlTasks())
		{
			atp.WaitAny(&ptsk);
			atp.Destroy(ptsk);
		}
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::PvUnittest_RecvTask
//
//	@doc:
//		Client-handling task
//
//---------------------------------------------------------------------------
void *
CSocketTest::PvUnittest_RecvTask
	(
	void *pv
	)
{
	CAutoSocketProxy asp(static_cast<CSocket*>(pv));
	CSocket *psocket = asp.Psocket();

	GPOS_ASSERT(psocket->FCheck());

	ULONG ulRemaining = ULONG_MAX;
	while (0 < ulRemaining)
	{
		// receive client message
		CHAR szRecv[GPOS_TEST_NET_BUFFER_SIZE];
		ULONG ulRecvLen = psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));

		GPOS_ASSERT(ulRecvLen == GPOS_SZ_LENGTH(GPOS_TEST_NET_CLIENT_MSG) + 1);
		GPOS_ASSERT(0 == clib::IStrCmp(GPOS_TEST_NET_CLIENT_MSG, szRecv));

		const CHAR *szSend = GPOS_TEST_NET_SERVER_MSG;
		psocket->Send((void*) szSend, GPOS_SZ_LENGTH(szSend) + 1);

		// receive client ID
		ulRecvLen = psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));
		psocket->Send((void*) szRecv, ulRecvLen);

		// receive iteration number
		ulRecvLen = psocket->UlRecv(szRecv, GPOS_ARRAY_SIZE(szRecv));
		psocket->Send((void*) szRecv, ulRecvLen);

		ULONG_PTR *ulp = (ULONG_PTR*) szRecv;
		ulRemaining = (ULONG)*ulp;

		GPOS_CHECK_ABORT;
	}

	// block until client closes connection
	while (psocket->FCheck())
	{
		clib::USleep(1000);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CSocketTest::CheckException
//
//	@doc:
//		Check for exception simulation
//
//---------------------------------------------------------------------------
void
CSocketTest::CheckException
	(
	CException ex
	)
{
	// on exception simulation, if an exception is injected in the server thread,
	// the thread first closes the listening socket and then propagates the exception;
	// a client thread may detect a networking error and be first to propagate
	// it to the main thread;
	// this is perfectly normal but breaks simulation as the framework
	// may expect an different exception type (OOM or Abort)
	if (ex.UlMinor() == CException::ExmiNetError)
	{
		if (GPOS_FTRACE(EtraceSimulateOOM))
		{
			GPOS_RESET_EX;
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiOOM);
		}

		if (GPOS_FTRACE(EtraceSimulateAbort))
		{
			GPOS_RESET_EX;
			GPOS_RAISE(CException::ExmaSystem, CException::ExmiAbort);
		}
	}
	GPOS_RETHROW(ex);
}


// EOF
