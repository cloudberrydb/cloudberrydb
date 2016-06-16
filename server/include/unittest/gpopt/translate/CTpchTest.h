//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTpchTest.h
//
//	@doc:
//		Test for translating DXL->Log-CExpression-> Phy-CExpression-> DXL
//		 for TPCH queries
//---------------------------------------------------------------------------
#ifndef GPOPT_CTpchTest_H
#define GPOPT_CTpchTest_H

#include "gpos/base.h"

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CTpchTest
	//
	//	@doc:
	//		Unittests
	//
	//---------------------------------------------------------------------------
	class CTpchTest
	{
		private:

			// metadata file
			static
			const CHAR *m_szTpchMDFileName;

			static
			ULONG m_ulTestCounter;

			static
			ULONG m_ulTestCounterParts;
		public:

			// unittests; 'ulQueryNum' indicates TPC-H query number in [1, 22]
			static GPOS_RESULT EresSubtest(ULONG ulQueryNum);

	}; // class CTpchTest
}

#endif // !GPOPT_CTpchTest_H

// EOF

