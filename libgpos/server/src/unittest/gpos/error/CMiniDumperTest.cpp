//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumperTest.cpp
//
//	@doc:
//		Tests for minidump handler
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/common/clibwrapper.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/io/COstreamString.h"
#include "gpos/memory/CAutoMemoryPool.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/task/CAutoSuspendAbort.h"
#include "gpos/task/CAutoTaskProxy.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/error/CMiniDumperTest.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::EresUnittest
//
//	@doc:
//		Function for testing minidump handler
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMiniDumperTest::EresUnittest()
{
	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMiniDumperTest::EresUnittest_Basic),
		GPOS_UNITTEST_FUNC(CMiniDumperTest::EresUnittest_Concurrency),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::EresUnittest_Basic
//
//	@doc:
//		Basic test for minidump handler
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMiniDumperTest::EresUnittest_Basic()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CMiniDumperStream mdrs(pmp);

	CWStringDynamic minidumpstr(pmp);
	COstreamString oss(&minidumpstr);
	mdrs.Init(&oss);

	GPOS_TRY
	{
		(void) PvRaise(NULL);
	}
	GPOS_CATCH_EX(ex)
	{
		mdrs.Finalize();

		GPOS_RESET_EX;

		GPOS_TRACE(minidumpstr.Wsz());
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::EresUnittest_Concurrency
//
//	@doc:
//		Test minidump handler for multiple executing threads
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMiniDumperTest::EresUnittest_Concurrency()
{
	CAutoMemoryPool amp;
	IMemoryPool *pmp = amp.Pmp();

	CWorkerPoolManager *pwpm = CWorkerPoolManager::Pwpm();

	CWStringDynamic minidumpstr(pmp);
	COstreamString oss(&minidumpstr);
	CMiniDumperStream mdrs(pmp);
	mdrs.Init(&oss);

	GPOS_TRY
	{
		// register stack serializer with error context
		CSerializableStack ss;

		CAutoTaskProxy atp(pmp, pwpm);
		CTask *rgPtsk[6];

		// one task throws, the other get aborted
		rgPtsk[0] = atp.PtskCreate(PvRaise, NULL);
		rgPtsk[1] = atp.PtskCreate(PvLoop, NULL);
		rgPtsk[2] = atp.PtskCreate(PvLoop, NULL);
		rgPtsk[3] = atp.PtskCreate(PvLoopSerialize, NULL);
		rgPtsk[4] = atp.PtskCreate(PvLoopSerialize, NULL);
		rgPtsk[5] = atp.PtskCreate(PvLoopSerialize, NULL);

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			atp.Schedule(rgPtsk[i]);
		}

		for (ULONG i = 0; i < GPOS_ARRAY_SIZE(rgPtsk); i++)
		{
			atp.Wait(rgPtsk[i]);
		}
	}
	GPOS_CATCH_EX(ex)
	{
		mdrs.Finalize();

		GPOS_RESET_EX;

		GPOS_TRACE(minidumpstr.Wsz());
	}
	GPOS_CATCH_END;

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::PvRaise
//
//	@doc:
//		Function raising an exception
//
//---------------------------------------------------------------------------
void *
CMiniDumperTest::PvRaise
	(
	void * // pv
	)
{
	// register stack serializer with error context
	CSerializableStack ss;

	clib::USleep(1000);

	// raise exception to trigger minidump
	GPOS_OOM_CHECK(NULL);

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::PvLoop
//
//	@doc:
//		Function waiting for abort
//
//---------------------------------------------------------------------------
void *
CMiniDumperTest::PvLoop
	(
	void * // pv
	)
{
	// loop until task is aborted
	while(true)
	{
		GPOS_CHECK_ABORT;

		clib::USleep(100);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::PvLoopSerialize
//
//	@doc:
//		Function waiting for abort
//
//---------------------------------------------------------------------------
void *
CMiniDumperTest::PvLoopSerialize
	(
	void * // pv
	)
{
	CSerializableStack ss;

	// loop until task is aborted
	while(true)
	{
		GPOS_CHECK_ABORT;

		clib::USleep(100);
	}

	return NULL;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::CMiniDumperStream
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMiniDumperTest::CMiniDumperStream::CMiniDumperStream
	(
	IMemoryPool *pmp
	)
	:
	CMiniDumper(pmp)
{}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::~CMiniDumperStream
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMiniDumperTest::CMiniDumperStream::~CMiniDumperStream()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::UlSerializeHeader
//
//	@doc:
//		Serialize minidump header
//
//---------------------------------------------------------------------------
void
CMiniDumperTest::CMiniDumperStream::SerializeHeader()
{
	*m_oos << "\n<MINIDUMP_TEST>\n";
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::UlSerializeFooter
//
//	@doc:
//		Serialize minidump footer
//
//---------------------------------------------------------------------------
void
CMiniDumperTest::CMiniDumperStream::SerializeFooter()
{
	*m_oos << "</MINIDUMP_TEST>\n";
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::SerializeEntryHeader
//
//	@doc:
//		Serialize entry header
//
//---------------------------------------------------------------------------
void
CMiniDumperTest::CMiniDumperStream::SerializeEntryHeader()
{
	WCHAR wszBuffer[GPOS_MINIDUMP_BUF_SIZE];
	CWStringStatic wstr(wszBuffer, GPOS_ARRAY_SIZE(wszBuffer));
	wstr.AppendFormat
		(
		GPOS_WSZ_LIT("<THREAD ID=%d>\n"),
		CWorker::PwrkrSelf()->UlThreadId()
		);

	*m_oos << wstr.Wsz();
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::SerializeEntryFooter
//
//	@doc:
//		Serialize entry footer
//
//---------------------------------------------------------------------------
void
CMiniDumperTest::CMiniDumperStream::SerializeEntryFooter
	(
	)
{
	*m_oos << "</THREAD>\n";
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CSerializableStack::CSerializableStack
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMiniDumperTest::CSerializableStack::CSerializableStack()
	:
	CSerializable()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CSerializableStack::~CSerializableStack
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------

CMiniDumperTest::CSerializableStack::~CSerializableStack()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CSerializableStack::Serialize
//
//	@doc:
//		Serialize object to passed stream
//
//---------------------------------------------------------------------------
void
CMiniDumperTest::CSerializableStack::Serialize
	(
	COstream& oos
	)
{
	WCHAR wszStackBuffer[GPOS_MINIDUMP_BUF_SIZE];
	CWStringStatic wstr(wszStackBuffer, GPOS_ARRAY_SIZE(wszStackBuffer));

	wstr.AppendFormat(GPOS_WSZ_LIT("<STACK_TRACE>\n"));

	CErrorContext *perrctxt = CTask::PtskSelf()->PerrctxtConvert();
	perrctxt->Psd()->AppendTrace(&wstr);

	wstr.AppendFormat(GPOS_WSZ_LIT("</STACK_TRACE>\n"));

	oos << wstr.Wsz();
}


// EOF
