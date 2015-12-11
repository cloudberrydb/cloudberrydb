//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLOperatorFactory.h
//
//	@doc:
//		Factory for creating DXL tree elements out of parsed XML attributes
//
//	@owner: 
//		n
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLOperatorFactory_INL
#define GPDXL_CDXLOperatorFactory_INL

namespace gpdxl
{
	using namespace gpos;
	using namespace gpdxl;

	// parse a comma-separated list of integers numbers into a dynamic array
	// will raise an exception if list is not well-formed
	template <typename T, void (*pfnDestroy)(T*),
			T ValueFromXmlstr(CDXLMemoryManager *, const XMLCh *, Edxltoken, Edxltoken)>
	CDynamicPtrArray<T, pfnDestroy> *
	CDXLOperatorFactory::PdrgptFromXMLCh
		(
		CDXLMemoryManager *pmm,
		const XMLCh *xmlszUlList,
		Edxltoken edxltokenAttr,
		Edxltoken edxltokenElement
		)
	{
		// get the memory pool from the memory manager
		IMemoryPool *pmp = pmm->Pmp();
	
		CDynamicPtrArray<T, pfnDestroy> *pdrgpt = GPOS_NEW(pmp) CDynamicPtrArray<T, pfnDestroy>(pmp);
		
		XMLStringTokenizer xmlsztok(xmlszUlList, CDXLTokens::XmlstrToken(EdxltokenComma));
		const ULONG ulNumTokens = xmlsztok.countTokens();
		
		for (ULONG ul = 0; ul < ulNumTokens; ul++)
		{
			XMLCh *xmlszNext = xmlsztok.nextToken();
			
			GPOS_ASSERT(NULL != xmlszNext);
			
			T *pt = GPOS_NEW(pmp) T(ValueFromXmlstr(pmm, xmlszNext, edxltokenAttr, edxltokenElement));
			pdrgpt->Append(pt);
		}
		
		return pdrgpt;	
	}
}

#endif // !GPDXL_CDXLOperatorFactory_INL

// EOF
