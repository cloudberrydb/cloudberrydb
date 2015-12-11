//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarOpList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing scalar
//		operator lists
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOpList.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarOpList.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOpList::CParseHandlerScalarOpList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarOpList::CParseHandlerScalarOpList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_edxloplisttype(CDXLScalarOpList::EdxloplistSentinel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOpList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarOpList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	CDXLScalarOpList::EdxlOpListType edxloplisttype = Edxloplisttype(xmlszLocalname);
	if (NULL == m_pdxln && CDXLScalarOpList::EdxloplistSentinel > edxloplisttype)
	{
		// create the list
		m_edxloplisttype = edxloplisttype;
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarOpList(m_pmp, m_edxloplisttype));
	}
	else
	{
		// we must have already initialized the list node
		GPOS_ASSERT(NULL != m_pdxln);

		// parse scalar child
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOpList::Edxloplisttype
//
//	@doc:
//		Return the op list type corresponding to the given operator name
//
//---------------------------------------------------------------------------
CDXLScalarOpList::EdxlOpListType
CParseHandlerScalarOpList::Edxloplisttype
	(
	const XMLCh* const xmlszLocalname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarOpList), xmlszLocalname))
	{
		return CDXLScalarOpList::EdxloplistGeneral;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartLevelEqFilterList), xmlszLocalname))
	{
		return CDXLScalarOpList::EdxloplistEqFilterList;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPartLevelFilterList), xmlszLocalname))
	{
		return CDXLScalarOpList::EdxloplistFilterList;
	}

	return CDXLScalarOpList::EdxloplistSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarOpList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarOpList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	CDXLScalarOpList::EdxlOpListType edxloplisttype = Edxloplisttype(xmlszLocalname);
	if (m_edxloplisttype != edxloplisttype)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// add constructed children from child parse handlers
	const ULONG ulSize = this->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
