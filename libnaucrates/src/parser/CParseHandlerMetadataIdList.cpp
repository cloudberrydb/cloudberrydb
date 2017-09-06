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
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pdrgpmdid(NULL)
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
	CRefCount::SafeRelease(m_pdrgpmdid);
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
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (FSupportedListType(xmlszLocalname))
	{
		// start of an index or partition metadata id list
		GPOS_ASSERT(NULL == m_pdrgpmdid);
		
		m_pdrgpmdid = GPOS_NEW(m_pmp) DrgPmdid(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndex), xmlszLocalname))
	{
		// index metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_pdrgpmdid);
		
		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenIndex);
		m_pdrgpmdid->Append(pmdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTrigger), xmlszLocalname))
	{
		// trigger metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_pdrgpmdid);

		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenTrigger);
		m_pdrgpmdid->Append(pmdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartition), xmlszLocalname))
	{
		// partition metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_pdrgpmdid);
		
		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenPartition);
		m_pdrgpmdid->Append(pmdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraint), xmlszLocalname))
	{
		// check constraint metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_pdrgpmdid);
		
		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenCheckConstraint);
		m_pdrgpmdid->Append(pmdid);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClass), xmlszLocalname))
	{
		// opclass metadata id: array must be initialized already
		GPOS_ASSERT(NULL != m_pdrgpmdid);
		
		IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenOpClass);
		m_pdrgpmdid->Append(pmdid);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
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
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTriggers), xmlszLocalname) ||
		0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartitions), xmlszLocalname)||
		0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraints), xmlszLocalname) ||
		0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClasses), xmlszLocalname))
	{
		// end the index or partition metadata id list
		GPOS_ASSERT(NULL != m_pdrgpmdid);

		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else if (!FSupportedElem(xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
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
	const XMLCh* const xmlsz
	)
{
	return (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndex), xmlsz) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTrigger), xmlsz)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartition), xmlsz)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraint), xmlsz)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClass), xmlsz));
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
	const XMLCh* const xmlsz
	)
{
	return (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTriggers), xmlsz)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartitions), xmlsz)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraints), xmlsz)  ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClasses), xmlsz));
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataIdList::Pdrgpmdid
//
//	@doc:
//		Return the constructed list of metadata ids
//
//---------------------------------------------------------------------------
DrgPmdid *
CParseHandlerMetadataIdList::Pdrgpmdid()
{
	return m_pdrgpmdid;
}
// EOF
