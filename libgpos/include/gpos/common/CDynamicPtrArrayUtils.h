//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDynamicPtrArrayUtils.h
//
//	@doc:
//		Utility functions for dynamic arrays of pointers
//
//	@owner:
//		onose
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOS_CDynamicPtrArrayUtils_H
#define GPOS_CDynamicPtrArrayUtils_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDynamicPtrArrayUtils
	//
	//	@doc:
	//		Utility functions for dynamic arrays of pointers
	//
	//---------------------------------------------------------------------------
	class CDynamicPtrArrayUtils
	{
		public:
			// return the indexes of first appearances of elements of the first array
			// in the second array if the first array is not included in the second,
			// return null
			// equality comparison between elements is via the "==" operator
			template <typename T, void (*pfnDestroy)(T*)>
			static
			DrgPul *PdrgpulSubsequenceIndexes
				(
				IMemoryPool *pmp,
				CDynamicPtrArray<T, pfnDestroy> *pdrgSubsequence,
				CDynamicPtrArray<T, pfnDestroy> *pdrg
				);
	};
}

#include "gpos/common/CDynamicPtrArrayUtils.inl"

#endif // !GPOS_CDynamicPtrArrayUtils_H

// EOF
