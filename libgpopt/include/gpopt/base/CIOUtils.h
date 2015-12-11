//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CIOUtils.h
//
//	@doc:
//		Optimizer I/O utility functions
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CIOUtils_H
#define GPOPT_CIOUtils_H

#include "gpos/base.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CIOUtils
	//
	//	@doc:
	//		Optimizer I/O utility functions
	//
	//---------------------------------------------------------------------------
	class CIOUtils
	{

		public:

			// dump given string to output file
			static
			void Dump(CHAR *szFileName, CHAR *sz);

	}; // class CIOUtils
}


#endif // !GPOPT_CIOUtils_H

// EOF
