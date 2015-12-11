//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CTranslatorExprToDXLTest.h
//
//	@doc:
//		Test for translating CExpression to DXL
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CTranslatorExprToDXLTest_H
#define GPOPT_CTranslatorExprToDXLTest_H

#include "gpos/base.h"


namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorExprToDXLTest
	//
	//	@doc:
	//		Unittests
	//
	//---------------------------------------------------------------------------
	class CTranslatorExprToDXLTest
	{
		private:

			// counter used to mark last successful test
			static ULONG m_ulTestCounter;

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_RunTests();
			static GPOS_RESULT EresUnittest_RunMinidumpTests();

	}; // class CTranslatorExprToDXLTest
}

#endif // !GPOPT_CTranslatorExprToDXLTest_H

// EOF

