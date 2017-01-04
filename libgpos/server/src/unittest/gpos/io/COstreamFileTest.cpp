//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		COstreamFileTest.cpp
//
//	@doc:
//		Tests for COstreamFile
//---------------------------------------------------------------------------

#include "gpos/io/ioutils.h"
#include "gpos/io/CFileReader.h"
#include "gpos/io/COstreamFile.h"
#include "gpos/string/CStringStatic.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/string/CWStringConst.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/io/COstreamFileTest.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		COstreamFileTest::EresUnittest
//
//	@doc:
//		Function for raising assert exceptions; again, encapsulated in a function
//		to facilitate debugging
//
//---------------------------------------------------------------------------
GPOS_RESULT
COstreamFileTest::EresUnittest()
{

	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(COstreamFileTest::EresUnittest_Basic),
		};

	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamFileTest::EresUnittest_Basic
//
//	@doc:
//		Basic test for writing to output file stream;
//
//---------------------------------------------------------------------------
GPOS_RESULT
COstreamFileTest::EresUnittest_Basic()
{
	// create temporary file in new directory under /tmp
	CHAR szPath[GPOS_FILE_NAME_BUF_SIZE];
	CHAR szFile[GPOS_FILE_NAME_BUF_SIZE];

	CStringStatic strPath(szPath, GPOS_ARRAY_SIZE(szPath));
	CStringStatic strFile(szFile, GPOS_ARRAY_SIZE(szFile));

	strPath.AppendBuffer("/tmp/gpos_test_stream.XXXXXX");

	// create dir
	(void) ioutils::SzMkDTemp(szPath);

	strFile.Append(&strPath);
	strFile.AppendBuffer("/COstreamFileTest");

	GPOS_TRY
	{
		Unittest_WriteFileStream(strFile.Sz());

		Unittest_CheckOutputFile(strFile.Sz());
	}
	GPOS_CATCH_EX(ex)
	{
		Unittest_DeleteTmpFile(strPath.Sz(), strFile.Sz());

		GPOS_RETHROW(ex);
	}
	GPOS_CATCH_END;

	Unittest_DeleteTmpFile(strPath.Sz(), strFile.Sz());

	return GPOS_OK;
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamFileTest::Unittest_WriteFileStream
//
//	@doc:
//		Write to output file stream
//
//---------------------------------------------------------------------------
void
COstreamFileTest::Unittest_WriteFileStream
	(
	const CHAR *szFile
	)
{
	GPOS_ASSERT(NULL != szFile);

	COstreamFile osf(szFile);

	const WCHAR wc = 'W';
	const CHAR c = 'C';
	const ULLONG ull = 102;
	const LINT li = -10;
	const WCHAR wsz[] = GPOS_WSZ_LIT("some regular string");
	const INT hex = 0xdeadbeef;

	osf
		<< wc
		<< c
		<< ull
		<< li
		<< wsz
		<< COstream::EsmHex
		<< hex
		;
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamFileTest::Unittest_CheckOutputFile
//
//	@doc:
//		Check the contents of the file used by the output stream
//
//---------------------------------------------------------------------------
void
COstreamFileTest::Unittest_CheckOutputFile
	(
	const CHAR *szFile
	)
{
	GPOS_ASSERT(NULL != szFile);

	CFileReader fr;
	fr.Open(szFile);

	const ULONG ulReadBufferSize = 1024;
	WCHAR wszReadBuffer[ulReadBufferSize];

#ifdef GPOS_DEBUG
	ULONG_PTR ulpRead =
#endif // GPOS_DEBUG
	fr.UlpRead((BYTE *) wszReadBuffer, GPOS_ARRAY_SIZE(wszReadBuffer));

	CWStringConst strExpected(GPOS_WSZ_LIT("WC102-10some regular stringdeadbeef"));

	GPOS_ASSERT(ulpRead == (ULONG_PTR) strExpected.UlLength() * GPOS_SIZEOF(WCHAR));
	GPOS_ASSERT(strExpected.FEquals(&strExpected));
}


//---------------------------------------------------------------------------
//	@function:
//		COstreamFileTest::Unittest_DeleteTmpFile
//
//	@doc:
//		Delete temporary file;
//
//---------------------------------------------------------------------------
void
COstreamFileTest::Unittest_DeleteTmpFile
	(
	const CHAR *szDir,
	const CHAR *szFile
	)
{
	GPOS_ASSERT(NULL != szDir);
	GPOS_ASSERT(NULL != szFile);

	CAutoTraceFlag atf(EtraceSimulateIOError, false);

	if (ioutils::FPathExist(szFile))
	{
		// delete temporary file
		ioutils::Unlink(szFile);
	}

	if (ioutils::FPathExist(szDir))
	{
		// delete temporary dir
		ioutils::RmDir(szDir);
	}
}


// EOF

