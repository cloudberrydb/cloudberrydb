//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CMemoryPoolTest.h
//
//	@doc:
//		Test for CMemoryPool
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryPoolTest_H
#define GPOS_CMemoryPoolTest_H

#include "gpos/base.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolTest
	//
	//	@doc:
	//		Unittest for CMemoryPool
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolTest
	{
		public:

			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_DeleteNULL();
	};
}

#endif // !GPOS_CMemoryPoolTest_H

// EOF

