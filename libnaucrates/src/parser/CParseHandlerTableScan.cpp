//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerTableScan.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing table scan operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerTableScan.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableScan::CParseHandlerTableScan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerTableScan::CParseHandlerTableScan
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
//		CParseHandlerTableScan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTableScan::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& // attrs
	)
{
	StartElement(xmlszLocalname, EdxltokenPhysicalTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableScan::StartElement
//
//	@doc:
//		Start element helper function
//
//---------------------------------------------------------------------------
void
CParseHandlerTableScan::StartElement
	(
	const XMLCh* const xmlszLocalname,
	Edxltoken edxltoken
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(edxltoken), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	if (EdxltokenPhysicalTableScan == edxltoken)
	{
		m_pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalTableScan(m_pmp);
	}
	else
	{
		GPOS_ASSERT(EdxltokenPhysicalExternalScan == edxltoken);
		m_pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalExternalScan(m_pmp);
	}

	// create child node parsers in reverse order of their expected occurrence

	// parse handler for table descriptor
	CParseHandlerBase *pphTD = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenTableDescr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphTD);

	// parse handler for the filter
	CParseHandlerBase *pphFilter = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphFilter);

	// parse handler for the proj list
	CParseHandlerBase *pphPrL = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
	m_pphm->ActivateParseHandler(pphPrL);

	//parse handler for the properties of the operator
	CParseHandlerBase *pphProp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
	m_pphm->ActivateParseHandler(pphProp);

	// store child parse handlers in array
	this->Append(pphProp);
	this->Append(pphPrL);
	this->Append(pphFilter);
	this->Append(pphTD);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableScan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTableScan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	EndElement(xmlszLocalname, EdxltokenPhysicalTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTableScan::EndElement
//
//	@doc:
//		End element helper function
//
//---------------------------------------------------------------------------
void
CParseHandlerTableScan::EndElement
	(
	const XMLCh* const xmlszLocalname,
	Edxltoken edxltoken
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(edxltoken), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphFilter = dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerTableDescr *pphTD = dynamic_cast<CParseHandlerTableDescr*>((*this)[3]);

	GPOS_ASSERT(NULL != pphTD->Pdxltabdesc());

	// set table descriptor
	CDXLTableDescr *pdxltabdesc = pphTD->Pdxltabdesc();
	pdxltabdesc->AddRef();
	m_pdxlop->SetTableDescriptor(pdxltabdesc);

	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, m_pdxlop);
	// set statictics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add constructed children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphFilter);

#ifdef GPOS_DEBUG
	m_pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

