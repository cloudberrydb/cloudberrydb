//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerSequence.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing sequence 
//		operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerSequence.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLPhysicalDynamicTableScan.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSequence::CParseHandlerSequence
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerSequence::CParseHandlerSequence
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerPhysicalOp(pmp, pphm, pphRoot),
	m_fInsideSequence(false)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSequence::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSequence::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (!m_fInsideSequence && 0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalSequence), xmlszLocalname))
	{
		// new sequence operator
		// parse handler for the proj list
		CParseHandlerBase *pphPrL =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarProjList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphPrL);

		//parse handler for the properties of the operator
		CParseHandlerBase *pphProp =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenProperties), m_pphm, this);
		m_pphm->ActivateParseHandler(pphProp);

		// store child parse handlers in array
		this->Append(pphProp);
		this->Append(pphPrL);
		m_fInsideSequence = true;
	}
	else
	{
		// child of the sequence operator
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, xmlszLocalname, m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);
		this->Append(pphChild);
		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}	
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerSequence::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerSequence::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPhysicalSequence), 
										xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct node from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
		
	CDXLPhysicalSequence *pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalSequence(m_pmp);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);	

	// set statistics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add project list
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	GPOS_ASSERT(NULL != pphPrL);
	AddChildFromParseHandler(pphPrL);
			
	const ULONG ulLen = this->UlLength();
	// add constructed children from child parse handlers
	for (ULONG ul = 2; ul < ulLen; ul++)
	{
		CParseHandlerPhysicalOp *pphChild = dynamic_cast<CParseHandlerPhysicalOp*>((*this)[ul]);
		GPOS_ASSERT(NULL != pphChild);
		AddChildFromParseHandler(pphChild);
	}

#ifdef GPOS_DEBUG
	pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF

