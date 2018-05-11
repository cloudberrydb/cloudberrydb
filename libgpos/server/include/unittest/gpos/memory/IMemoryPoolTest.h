//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		IMemoryPoolTest.h
//
//	@doc:
//		Test for IMemoryPool
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_IMemoryPoolTest_H
#define GPOS_IMemoryPoolTest_H

#include "gpos/base.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMemoryPoolTest
	//
	//	@doc:
	//		Unittest for IMemoryPool
	//
	//---------------------------------------------------------------------------
	class CMemoryPoolTest
	{
		public:

			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_DeleteNULL();
	};
}

#endif // !GPOS_IMemoryPoolTest_H

// EOF

