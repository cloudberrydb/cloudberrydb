//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		IMemoryPoolTest.cpp
//
//	@doc:
//		Tests for IMemoryPool
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CException.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/memory/CMemoryPoolManager.h"
#include "gpos/task/CWorkerPoolManager.h"
#include "gpos/test/CUnittest.h"

#include "unittest/gpos/memory/IMemoryPoolTest.h"

using namespace gpos;


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPoolTest::EresUnittest
//
//	@doc:
//		unit test driver
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolTest::EresUnittest()
{

	CUnittest rgut[] =
		{
		GPOS_UNITTEST_FUNC(CMemoryPoolTest::EresUnittest_DeleteNULL)
		};

	// execute unit tests
	return CUnittest::EresExecute(rgut, GPOS_ARRAY_SIZE(rgut));

}


//---------------------------------------------------------------------------
//	@function:
//		CMemoryPool::EresUnittest_DeleteNULL
//
//	@doc:
//		unit test to check for deletion of NULL pointer
//
//---------------------------------------------------------------------------
GPOS_RESULT
CMemoryPoolTest::EresUnittest_DeleteNULL()
{
	char *sz = NULL;
	GPOS_DELETE(sz);

	return GPOS_OK;
}

// EOF

