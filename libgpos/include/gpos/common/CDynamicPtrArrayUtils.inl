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
#ifndef GPOS_CDynamicPtrArrayUtils_INL
#define GPOS_CDynamicPtrArrayUtils_INL

namespace gpos
{
	
	//---------------------------------------------------------------------------
	//	@function:
	//		CDynamicPtrArrayUtils::PdrgpulSubsequenceIndexes
	//
	//	@doc:
	//		Return the indexes of first appearances of elements of the first array
	//		in the second array if the first array is not included in the second,
	//		return null. For instance, if the first array is A,B,C and the second
	//		one is A,C,B,B, the function will return 0,2,1.
	//
	//---------------------------------------------------------------------------
	template <typename T, void (*pfnDestroy)(T*)>
	DrgPul *
	CDynamicPtrArrayUtils::PdrgpulSubsequenceIndexes
		(
		IMemoryPool *pmp,
		CDynamicPtrArray<T, pfnDestroy> *pdrgSubsequence,
		CDynamicPtrArray<T, pfnDestroy> *pdrg
		)
	{
		GPOS_ASSERT(NULL != pdrgSubsequence);
		GPOS_ASSERT(NULL != pdrg);

		ULONG ulSubsequence = pdrgSubsequence->UlLength();
		ULONG ulSequence = pdrg->UlLength();
		DrgPul *pdrgpulIndexes = GPOS_NEW(pmp) DrgPul(pmp);

		for (ULONG ul1 = 0; ul1 < ulSubsequence; ul1++)
		{
			T* pT = (*pdrgSubsequence)[ul1];
			BOOL fFound = false;
			for (ULONG ul2 = 0; ul2 < ulSequence; ul2++)
			{
				if (pT == (*pdrg)[ul2])
				{
					pdrgpulIndexes->Append(GPOS_NEW(pmp) ULONG(ul2));
					fFound = true;
					break;
				}
			}

			if (!fFound)
			{
				pdrgpulIndexes->Release();
				return NULL;
			}
		}

		return pdrgpulIndexes;
	}	

}

#endif // !GPOS_CDynamicPtrArrayUtils_INL

// EOF
