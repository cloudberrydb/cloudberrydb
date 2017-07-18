//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerScalarValuesList.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing value list.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarValuesList.h"

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLScalarValuesList.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

// ctor
CParseHandlerScalarValuesList::CParseHandlerScalarValuesList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerOp(pmp, pphm, pphRoot)
{
}

// invoked by Xerces to process an opening tag
void
CParseHandlerScalarValuesList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarValuesList), xmlszLocalname))
	{
		CDXLScalarValuesList *pdxlop = GPOS_NEW(m_pmp) CDXLScalarValuesList(m_pmp);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarConstValue), xmlszLocalname))
	{
		CParseHandlerBase *pphScConstValue = CParseHandlerFactory::Pph(m_pmp, xmlszLocalname, m_pphm, this);
		m_pphm->ActivateParseHandler(pphScConstValue);

		this->Append(pphScConstValue);

		pphScConstValue->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// invoked by Xerces to process a closing tag
void
CParseHandlerScalarValuesList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const, //xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{

	const ULONG ulArity = this->UlLength();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}
	m_pphm->DeactivateHandler();

}
// EOF
