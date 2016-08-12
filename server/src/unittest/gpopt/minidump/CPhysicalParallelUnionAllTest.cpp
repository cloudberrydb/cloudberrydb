//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.


#include "unittest/gpopt/minidump/CPhysicalParallelUnionAllTest.h"
#include "unittest/gpopt/CTestUtils.h"
#include "gpos/test/CUnittest.h"
#include "gpos/task/CAutoTraceFlag.h"
#include "naucrates/traceflags/traceflags.h"

using namespace gpos;

static ULONG ulCounter = 0;

static const CHAR *rgszFileNames[] =
	{
		"../data/dxl/minidump/CPhysicalParallelUnionAllTest/TwoHashedTables.mdp",
		"../data/dxl/minidump/CPhysicalParallelUnionAllTest/ParallelUnion-Insert.mdp",
		"../data/dxl/minidump/CPhysicalParallelUnionAllTest/ParallelUnion-ConstTable.mdp",
	};

namespace gpopt
{
	GPOS_RESULT CPhysicalParallelUnionAllTest::EresUnittest()
	{
		CAutoTraceFlag atfParallelAppend(gpos::EopttraceEnableParallelAppend, true);
		return CTestUtils::EresUnittest_RunTests
			(
				rgszFileNames,
				&ulCounter,
				GPOS_ARRAY_SIZE(rgszFileNames)
			);
	}

}
