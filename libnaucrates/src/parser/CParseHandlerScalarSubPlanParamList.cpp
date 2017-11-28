//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarSubPlanParamList.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing the ParamList
//		of a scalar SubPlan
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarSubPlanParamList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarSubPlanParam.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanParamList::CParseHandlerScalarSubPlanParamList
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubPlanParamList::CParseHandlerScalarSubPlanParamList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot)
{
	m_pdrgdxlcr = GPOS_NEW(pmp) DrgPdxlcr(pmp);
	m_fParamList = false;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanParamList::~CParseHandlerScalarSubPlanParamList
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerScalarSubPlanParamList::~CParseHandlerScalarSubPlanParamList()
{
	m_pdrgdxlcr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanParamList::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubPlanParamList::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes &attrs
	)
{
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanParamList), xmlszLocalname))
	{
		// we can't have seen a paramlist already
		GPOS_ASSERT(!m_fParamList);
		// start the paramlist
		m_fParamList = true;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanParam), xmlszLocalname))
	{
		// we must have seen a paramlist already
		GPOS_ASSERT(m_fParamList);

		// start new param
		CParseHandlerBase *pphParam = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanParam), m_pphm, this);
		m_pphm->ActivateParseHandler(pphParam);

		// store parse handler
		this->Append(pphParam);

		pphParam->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarSubPlanParamList::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarSubPlanParamList::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarSubPlanParamList), xmlszLocalname) && NULL != m_pdxln)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarSubPlanParam *pphParam = dynamic_cast<CParseHandlerScalarSubPlanParam *>((*this)[ul]);

		CDXLColRef *pdxlcr = pphParam->Pdxlcr();
		pdxlcr->AddRef();
		m_pdrgdxlcr->Append(pdxlcr);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
