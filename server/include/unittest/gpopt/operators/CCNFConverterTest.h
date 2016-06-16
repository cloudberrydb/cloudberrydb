//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCNFConverterTest.h
//
//	@doc:
//		Test for CNF conversion
//---------------------------------------------------------------------------
#ifndef GPOPT_CCNFConverterTest_H
#define GPOPT_CCNFConverterTest_H

#include "gpos/base.h"


namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCNFConverterTest
	//
	//	@doc:
	//		Unittests for CNF conversion
	//
	//---------------------------------------------------------------------------
	class CCNFConverterTest
	{

		public:

			// unittests
			static GPOS_RESULT EresUnittest();
			static GPOS_RESULT EresUnittest_Basic();
			static GPOS_RESULT EresUnittest_AvoidCNFConversion();

			// shorthand for functions for generating test cases
			typedef CExpression *(PexprTestCase)(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// test case for join evaluation
			struct SCNConverterSTestCase
			{
				// expression generation function pointer
				PexprTestCase *m_pfFst;

				// expression generation function pointer
				PexprTestCase *m_pfSnd;

				// should apply CNF expression
				BOOL m_fCNFConversion;

			}; // SCNConverterSTestCase

			// scalar expression comparing scalar ident to a constant
			static
			CExpression *PexprScIdentCmpConst(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar ident to a constant
			static
			CExpression *PexprAnd1(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar identifiers
			static
			CExpression *PexprAnd2(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar identifiers
			static
			CExpression *PexprAnd3(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar identifiers
			static
			CExpression *PexprAnd4(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar identifiers
			static
			CExpression *PexprAnd5(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar identifiers
			static
			CExpression *PexprAnd6(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// AND of scalar expression comparing scalar identifiers
			static
			CExpression *PexprAnd7(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// OR of scalar expression comparing scalar ident to a constant
			static
			CExpression *PexprOr(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

			// scalar expression comparing scalar identifiers
			static
			CExpression *PexprScIdentCmpScIdent(IMemoryPool *pmp, CExpression *pexprLeft, CExpression *pexprRight);

	}; // class CCNFConverterTest
}

#endif // !GPOPT_CCNFConverterTest_H

// EOF
