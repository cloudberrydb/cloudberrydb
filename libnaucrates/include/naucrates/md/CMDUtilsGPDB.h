//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Inc.
//
//	@filename:
//		CMDUtilsGPDB.h
//
//	@doc:
//		Utility functions that are used by GPDB related metadata
//
//---------------------------------------------------------------------------

#ifndef GPMD_CMDUtilsGPDB_H
#define GPMD_CMDUtilsGPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDRelation.h"

namespace gpmd
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDUtilsGPDB
	//
	//	@doc:
	//		Utility functions that are used by GPDB related metadata
	//
	//---------------------------------------------------------------------------
	class CMDUtilsGPDB
	{
		public:

			// initialize the output arguments based on the array of column metadata
			static
			void InitializeMDColInfo
				(
				IMemoryPool *pmp,
				DrgPmdcol *pdrgpmdcol,
				HMIUl *phmiulAttno2Pos, // output hash map of column attribute number to position in array of column metadata
				DrgPul *pdrgpulNonDroppedCols, // output array of non-dropped columns
				HMUlUl *phmululNonDroppedCols // output hash map of column array position mapping to position array of non-dropped columns
				);

	};

}

#endif // !GPMD_CMDUtilsGPDB_H

// EOF
