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
	mdrs.Init();

	GPOS_TRY
	{
		(void) PvRaise(NULL);
	}
	GPOS_CATCH_EX(ex)
	{
		mdrs.Finalize();

		GPOS_RESET_EX;

		CWStringDynamic wstr(pmp);
		COstreamString oss(&wstr);

		oss << mdrs;

		GPOS_TRACE(wstr.Wsz());

		// check conversion to multi-byte string
#ifdef GPOS_DEBUG
		ULONG_PTR ulp =
#endif // GPOS_DEBUG
		mdrs.UlpConvertToMultiByte();

		GPOS_ASSERT(ulp == wstr.UlLength());

		std::cout << reinterpret_cast<const CHAR*>(mdrs.Pb());
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

	CMiniDumperStream mdrs(pmp);
	mdrs.Init();

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

		CWStringDynamic wstr(pmp);
		COstreamString oss(&wstr);

		oss << mdrs;

		GPOS_TRACE(wstr.Wsz());
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
//		CMiniDumperTest::CMiniDumperStream::UlpRequiredSpaceEntryHeader
//
//	@doc:
//		Size to reserve for entry header
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperTest::CMiniDumperStream::UlpRequiredSpaceEntryHeader()
{
	WCHAR rgbBuffer[GPOS_MINIDUMP_BUF_SIZE];

	return UlpSerializeEntryHeader(rgbBuffer, GPOS_ARRAY_SIZE(rgbBuffer));
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::UlpRequiredSpaceEntryFooter
//
//	@doc:
//		Size to reserve for entry footer
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperTest::CMiniDumperStream::UlpRequiredSpaceEntryFooter()
{
	WCHAR rgbBuffer[GPOS_MINIDUMP_BUF_SIZE];

	return UlpSerializeEntryFooter(rgbBuffer, GPOS_ARRAY_SIZE(rgbBuffer));
}


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
	const WCHAR *wsz = GPOS_WSZ_LIT("\n<MINIDUMP_TEST>\n");
	ULONG ulSize = GPOS_WSZ_LENGTH(wsz);

	WCHAR *wszHeader = WszReserve(ulSize);
	if (NULL != wszHeader)
	{
		(void) clib::PvMemCpy(wszHeader, (BYTE *) wsz, ulSize * GPOS_SIZEOF(WCHAR));
	}
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
	const WCHAR *wsz = GPOS_WSZ_LIT("</MINIDUMP_TEST>\n");
	ULONG ulSize = GPOS_WSZ_LENGTH(wsz);

	WCHAR *wszFooter = WszReserve(ulSize);
	if (NULL != wszFooter)
	{
		(void) clib::PvMemCpy(wszFooter, (BYTE *) wsz, ulSize * GPOS_SIZEOF(WCHAR));
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::UlpSerializeEntryHeader
//
//	@doc:
//		Serialize entry header
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperTest::CMiniDumperStream::UlpSerializeEntryHeader
	(
	WCHAR *wszEntry,
	ULONG_PTR ulpAllocSize
	)
{
	WCHAR wszBuffer[GPOS_MINIDUMP_BUF_SIZE];
	CWStringStatic wstr(wszBuffer, GPOS_ARRAY_SIZE(wszBuffer));
	wstr.AppendFormat
		(
		GPOS_WSZ_LIT("<THREAD ID=%d>\n"),
		CWorker::PwrkrSelf()->UlThreadId()
		);

	ULONG_PTR ulpSize = std::min
		(
		(ULONG_PTR) wstr.UlLength(),
		ulpAllocSize
		);
	(void) clib::PvMemCpy(wszEntry, (WCHAR *) wszBuffer, ulpSize * GPOS_SIZEOF(WCHAR));

	return ulpSize;
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CMiniDumperStream::UlpSerializeEntryFooter
//
//	@doc:
//		Serialize entry footer
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperTest::CMiniDumperStream::UlpSerializeEntryFooter
	(
	WCHAR *wszEntry,
	ULONG_PTR ulpAllocSize
	)
{
	const WCHAR *wsz = GPOS_WSZ_LIT("</THREAD>\n");
	ULONG_PTR ulpSize = std::min
		(
		(ULONG_PTR) GPOS_WSZ_LENGTH(wsz),
		ulpAllocSize
		);

	(void) clib::PvMemCpy(wszEntry, (WCHAR *) wsz, ulpSize * GPOS_SIZEOF(WCHAR));

	return ulpSize;
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
//		CMiniDumperTest::CSerializableStack::UlpRequiredSpace
//
//	@doc:
//		Calculate space needed for serialization
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperTest::CSerializableStack::UlpRequiredSpace()
{
	return UlpSerialize(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer));
}


//---------------------------------------------------------------------------
//	@function:
//		CMiniDumperTest::CSerializableStack::UlpSerialize
//
//	@doc:
//		Serialize object to passed buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CMiniDumperTest::CSerializableStack::UlpSerialize
	(
	WCHAR *wszEntry,
	ULONG_PTR ulpAllocSize
	)
{
	WCHAR wszStackBuffer[GPOS_MINIDUMP_BUF_SIZE];
	CWStringStatic wstr(wszStackBuffer, GPOS_ARRAY_SIZE(wszStackBuffer));

	wstr.AppendFormat(GPOS_WSZ_LIT("<STACK_TRACE>\n"));

	CErrorContext *perrctxt = CTask::PtskSelf()->PerrctxtConvert();
	perrctxt->Psd()->AppendTrace(&wstr);

	wstr.AppendFormat(GPOS_WSZ_LIT("</STACK_TRACE>\n"));

	ULONG_PTR ulpSize = std::min
		(
		(ULONG_PTR) wstr.UlLength(),
		ulpAllocSize
		);

	(void) clib::PvMemCpy
		(
		(BYTE *) wszEntry,
		(BYTE *) wszStackBuffer,
		ulpSize * GPOS_SIZEOF(WCHAR)
		);

	return ulpSize;
}


// EOF
