//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLimit.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing Limit operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLimit.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLimit::CParseHandlerLimit
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerLimit::CParseHandlerLimit
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_pdxlop(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLimit::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLimit::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalLimit), xmlszLocalname))
	{

		// parse and create Limit operator
		m_pdxlop = (CDXLPhysicalLimit *) CDXLOperatorFactory::PdxlopLimit(m_pphm->Pmm(), attrs);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);

		// create and activate the parse handler for the children nodes in reverse
		// order of their expected appearance

		CParseHandlerBase *pphOffset = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarLimitOffset), m_pphm, this);
		m_pphm->ActivateParseHandler(pphOffset);

		CParseHandlerBase *pphCount = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarLimitCount), m_pphm, this);
		m_pphm->ActivateParseHandler(pphCount);

		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// parse handler for the proj list
		CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphPrL);

		//parse handler for the properties of the operator
		CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
		m_pphm->ActivateParseHandler(pphProp);

		// store parse handlers
		this->Append(pphProp);
		this->Append(pphPrL);
		this->Append(pphChild);
		this->Append(pphCount);
		this->Append(pphOffset);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLimit::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLimit::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalLimit), xmlszLocalname))
	{
		GPOS_ASSERT(5 == this->UlLength());

		// construct node from the created child nodes
		CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);

		CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
		CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[2]);
		CParseHandlerScalarOp *pphCount = dynamic_cast<CParseHandlerScalarOp *>((*this)[3]);
		CParseHandlerScalarOp *pphOffSet = dynamic_cast<CParseHandlerScalarOp *>((*this)[4]);
		
		// set statistics and physical properties
		CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

		// add constructed children
		AddChildFromParseHandler(pphPrL);
		AddChildFromParseHandler(pphChild);
		AddChildFromParseHandler(pphCount);
		AddChildFromParseHandler(pphOffSet);

		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// EOF
