//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumTest.h
//
//	@doc:
//		Test for datum classes
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CDatumTest_H
#define GPNAUCRATES_CDatumTest_H

#include "gpos/base.h"
#include "naucrates/base/IDatum.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDatumTest
	//
	//	@doc:
	//		Static unit tests for datum
	//
	//---------------------------------------------------------------------------
	class CDatumTest
	{

		private:
			// create an oid datum
			static
			IDatum *CreateOidDatum(IMemoryPool *mp, BOOL is_null);

			// create an int2 datum
			static
			IDatum *CreateInt2Datum(IMemoryPool *mp, BOOL is_null);

			// create an int4 datum
			static
			IDatum *CreateInt4Datum(IMemoryPool *mp, BOOL is_null);
		
			// create an int8 datum
			static
			IDatum *CreateInt8Datum(IMemoryPool *mp, BOOL is_null);

			// create a bool datum
			static
			IDatum *CreateBoolDatum(IMemoryPool *mp, BOOL is_null);

			// create a generic datum
			static
			IDatum *CreateGenericDatum(IMemoryPool *mp, BOOL is_null);

		public:

			// unittests
			static 
			GPOS_RESULT EresUnittest();
			
			static 
			GPOS_RESULT EresUnittest_Basics();

	}; // class CDatumTest
}

#endif // !GPNAUCRATES_CDatumTest_H


// EOF
