//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataIdList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing lists of metadata ids,
//		for example in the specification of the indices or partition tables for a relation.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::CParseHandlerMetadataIdList
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataIdList::CParseHandlerMetadataIdList
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
	:
	CParseHandlerBase(mp, parse_handler_mgr, parse_handler_root),
	m_mdid_array(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::~CParseHandlerMetadataIdList
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataIdList::~CParseHandlerMetadataIdList()
{
	CRefCount::SafeRelease(m_mdid_array);
}



//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMetadataIdList::StartElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const, // element_qname
	const Attributes& attrs
	)
{
	if (FSupportedListType(element_local_name))
	{
		// start of an index or partition metadata id list
		GPOS_ASSERT(NULL == m_mdid_array);
		
		m_mdid_array = GPOS_NEW(m_mp) IMdIdArray(m_mp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndex), element_local_name))
	{
		// index metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_mdid_array);
		
		IMDId *mdid = CDXLOperatorFactory::ExtractConvertAttrValueToMdId(m_parse_handler_mgr->GetDXLMemoryManager(), attrs, EdxltokenMdid, EdxltokenIndex);
		m_mdid_array->Append(mdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTrigger), element_local_name))
	{
		// trigger metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_mdid_array);

		IMDId *mdid = CDXLOperatorFactory::ExtractConvertAttrValueToMdId(m_parse_handler_mgr->GetDXLMemoryManager(), attrs, EdxltokenMdid, EdxltokenTrigger);
		m_mdid_array->Append(mdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartition), element_local_name))
	{
		// partition metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_mdid_array);
		
		IMDId *mdid = CDXLOperatorFactory::ExtractConvertAttrValueToMdId(m_parse_handler_mgr->GetDXLMemoryManager(), attrs, EdxltokenMdid, EdxltokenPartition);
		m_mdid_array->Append(mdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraint), element_local_name))
	{
		// check constraint metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_mdid_array);
		
		IMDId *mdid = CDXLOperatorFactory::ExtractConvertAttrValueToMdId(m_parse_handler_mgr->GetDXLMemoryManager(), attrs, EdxltokenMdid, EdxltokenCheckConstraint);
		m_mdid_array->Append(mdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClass), element_local_name))
	{
		// opclass metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_mdid_array);
		
		IMDId *mdid = CDXLOperatorFactory::ExtractConvertAttrValueToMdId(m_parse_handler_mgr->GetDXLMemoryManager(), attrs, EdxltokenMdid, EdxltokenOpClass);
		m_mdid_array->Append(mdid);
	}
	else
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, str->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMetadataIdList::EndElement
	(
	const XMLCh* const, // element_uri,
	const XMLCh* const element_local_name,
	const XMLCh* const // element_qname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTriggers), element_local_name) ||
		0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartitions), element_local_name)||
		0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraints), element_local_name) ||
		0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClasses), element_local_name))
	{
		// end the index or partition metadata id list
		GPOS_ASSERT(NULL != m_mdid_array);

		// deactivate handler
		m_parse_handler_mgr->DeactivateHandler();
	}
	else if (!FSupportedElem(element_local_name))
	{
		CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(m_parse_handler_mgr->GetDXLMemoryManager(), element_local_name);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, str->GetBuffer());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::FSupportedElem
//
//	@doc:
//		Is this a supported MD list elem
//
//---------------------------------------------------------------------------
BOOL
CParseHandlerMetadataIdList::FSupportedElem
	(
	const XMLCh* const xml_str
	)
{
	return (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndex), xml_str) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTrigger), xml_str)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartition), xml_str)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraint), xml_str)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClass), xml_str));
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::FSupportedListType
//
//	@doc:
//		Is this a supported MD list type
//
//---------------------------------------------------------------------------
BOOL
CParseHandlerMetadataIdList::FSupportedListType
	(
	const XMLCh* const xml_str
	)
{
	return (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTriggers), xml_str)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartitions), xml_str)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraints), xml_str)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClasses), xml_str));
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::GetMdIdArray
//
//	@doc:
//		Return the constructed list of metadata ids
//
//---------------------------------------------------------------------------
IMdIdArray *
CParseHandlerMetadataIdList::GetMdIdArray()
{
	return m_mdid_array;
}
// EOF
