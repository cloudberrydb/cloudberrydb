//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDCacheObject.cpp
//
//	@doc:
//		Implementation of common methods for MD cache objects
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDCacheObject.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDCacheObject::SerializeMDIdAsElem
//
//	@doc:
//		Serialize MD operator info in DXL format
//
//---------------------------------------------------------------------------
void
IMDCacheObject::SerializeMDIdAsElem
	(
	CXMLSerializer *pxmlser,
	const CWStringConst *pstrElem,
	const IMDId *pmdid
	)
	const
{
	if (NULL == pmdid)
	{
		return;
	}
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						pstrElem);
	
	pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
							pstrElem);
}


//---------------------------------------------------------------------------
//	@function:
//		IMDCacheObject::SerializeMDIdList
//
//	@doc:
//		Serialize a list of metadata ids into DXL
//
//---------------------------------------------------------------------------
void
IMDCacheObject::SerializeMDIdList
	(
	CXMLSerializer *pxmlser,
	const DrgPmdid *pdrgpmdid,
	const CWStringConst *pstrTokenList,
	const CWStringConst *pstrTokenListItem
	)
{
	// serialize list of metadata ids
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenList);
	const ULONG ulLen = pdrgpmdid->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenListItem);

		IMDId *pmdid = (*pdrgpmdid)[ul];
		pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenListItem);

		GPOS_CHECK_ABORT;
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrTokenList);
}

// EOF

