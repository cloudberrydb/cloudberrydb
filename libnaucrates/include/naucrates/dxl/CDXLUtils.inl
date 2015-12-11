//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLUtils.inl
//
//	@doc:
//		Entry point for parsing and serializing DXL documents.
//
//	@owner: 
//		n
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLUtils_INL
#define GPDXL_CDXLUtils_INL

namespace gpdxl
{
	using namespace gpos;

	// serialize a list of integers into a comma-separate string
	template <typename T, void (*pfnDestroy)(T*)>
	CWStringDynamic *
	CDXLUtils::PstrSerialize
		(
		IMemoryPool *pmp,
		const CDynamicPtrArray<T, pfnDestroy> *pdrgpt
		)
	{
		CAutoP<CWStringDynamic> a_pstr(GPOS_NEW(pmp) CWStringDynamic(pmp));
	
		ULONG ulLength = pdrgpt->UlLength();
		for (ULONG ul = 0; ul < ulLength; ul++)
		{
			T tValue = *((*pdrgpt)[ul]);
			if (ul == ulLength - 1)
			{
				// last element: do not print a comma
				a_pstr->AppendFormat(GPOS_WSZ_LIT("%d"), tValue);
			}
			else
			{
				a_pstr->AppendFormat(GPOS_WSZ_LIT("%d%ls"), tValue, CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
			}
		}
	
		return a_pstr.PtReset();			
	}

}

#endif // GPDXL_CDXLUtils_INL

// EOF
