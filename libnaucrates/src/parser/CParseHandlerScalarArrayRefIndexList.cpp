//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarArrayRefIndexList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing arrayref indexes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarArrayRefIndexList.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarArrayRefIndexList.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRefIndexList::CParseHandlerScalarArrayRefIndexList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarArrayRefIndexList::CParseHandlerScalarArrayRefIndexList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRefIndexList::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayRefIndexList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexList), xmlszLocalname))
	{
		// start the index list
		const XMLCh *xmlszOpType = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenScalarArrayRefIndexListBound,
															EdxltokenScalarArrayRefIndexList
															);

		CDXLScalarArrayRefIndexList::EIndexListBound eilb = CDXLScalarArrayRefIndexList::EilbUpper;
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexListLower), xmlszOpType))
		{
			eilb = CDXLScalarArrayRefIndexList::EilbLower;
		}
		else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexListUpper), xmlszOpType))
		{
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiDXLInvalidAttributeValue,
				CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexListBound)->Wsz(),
				CDXLTokens::PstrToken(EdxltokenScalarArrayRefIndexList)->Wsz()
				);
		}

		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarArrayRefIndexList(m_pmp, eilb));
	}
	else
	{
		// we must have seen a index list already and initialized the index list node
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
//		CParseHandlerScalarArrayRefIndexList::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayRefIndexList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexList), xmlszLocalname))
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
