//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerPlan.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing physical plans.
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/parser/CParseHandlerDirectDispatchInfo.h"
#include "naucrates/dxl/parser/CParseHandlerPlan.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPlan::CParseHandlerPlan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerPlan::CParseHandlerPlan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_ullId(0),
	m_ullSpaceSize(0),
	m_pdxln(NULL),
	m_pdxlddinfo(NULL)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPlan::~CParseHandlerPlan
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerPlan::~CParseHandlerPlan()
{
	CRefCount::SafeRelease(m_pdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPlan::Pdxln
//
//	@doc:
//		Root of constructed DXL plan
//
//---------------------------------------------------------------------------
CDXLNode *
CParseHandlerPlan::Pdxln()
{
	return m_pdxln;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPlan::Edxlphtype
//
//	@doc:
//		Parse handler type
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerPlan::Edxlphtype() const
{
	return EdxlphPlan;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPlan::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPlan::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes &attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenDirectDispatchInfo), xmlszLocalname))
	{
		GPOS_ASSERT(0 < this->UlLength());
		CParseHandlerBase *pphDirectDispatch = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenDirectDispatchInfo), m_pphm, this);
		m_pphm->ActivateParseHandler(pphDirectDispatch);
		
		// store parse handler
		this->Append(pphDirectDispatch);
		pphDirectDispatch->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		return;
	}
	
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPlan), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse plan id
	const XMLCh *xmlszPlanId = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenPlanId, EdxltokenPlan);
	m_ullId = CDXLOperatorFactory::UllValueFromXmlstr(m_pphm->Pmm(), xmlszPlanId, EdxltokenPlanId, EdxltokenPlan);

	// parse plan space size
	const XMLCh *xmlszPlanSpaceSize = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenPlanSpaceSize, EdxltokenPlan);
	m_ullSpaceSize = CDXLOperatorFactory::UllValueFromXmlstr(m_pphm->Pmm(), xmlszPlanSpaceSize, EdxltokenPlanSpaceSize, EdxltokenPlan);

	// create a parse handler for physical nodes and activate it
	GPOS_ASSERT(NULL != m_pmp);
	CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenPhysical), m_pphm, this);
	m_pphm->ActivateParseHandler(pph);
	
	// store parse handler
	this->Append(pph);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPlan::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerPlan::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenPlan), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	CParseHandlerPhysicalOp *pph = dynamic_cast<CParseHandlerPhysicalOp *>((*this)[0]);
	
	GPOS_ASSERT(NULL != pph->Pdxln());

	// store constructed child
	m_pdxln = pph->Pdxln();
	m_pdxln->AddRef();
	
	if (2 == this->UlLength())
	{
		CParseHandlerDirectDispatchInfo *pphDirectDispatchInfo = dynamic_cast<CParseHandlerDirectDispatchInfo *>((*this)[1]);
		CDXLDirectDispatchInfo *pdxlddinfo = pphDirectDispatchInfo->Pdxlddinfo();
		GPOS_ASSERT(NULL != pdxlddinfo);
		
		pdxlddinfo->AddRef();
		m_pdxln->SetDirectDispatchInfo(pdxlddinfo);
	}
	// deactivate handler

	m_pphm->DeactivateHandler();
}

// EOF

