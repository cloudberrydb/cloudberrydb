//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerPhysicalAbstractBitmapScan.h
//
//	@doc:
//		SAX parse handler parent class for parsing bitmap scan operator nodes
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerPhysicalAbstractBitmapScan.h"

#include "naucrates/dxl/operators/CDXLPhysicalAbstractBitmapScan.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFilter.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalBitmapTableScan.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalDynamicBitmapTableScan.h"
#include "naucrates/dxl/parser/CParseHandlerProjList.h"
#include "naucrates/dxl/parser/CParseHandlerProperties.h"
#include "naucrates/dxl/parser/CParseHandlerScalarBitmapIndexProbe.h"
#include "naucrates/dxl/parser/CParseHandlerTableDescr.h"
#include "naucrates/dxl/parser/CParseHandlerUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalAbstractBitmapScan::StartElementHelper
//
//	@doc:
//		Common StartElement functionality for children of this class
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalAbstractBitmapScan::StartElementHelper
	(
 	const XMLCh* const xmlszLocalname,
	Edxltoken edxltoken
	)
{
	if (0 != XMLString::compareString
				(
				CDXLTokens::XmlstrToken(edxltoken),
				xmlszLocalname
				))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// create child node parsers in reverse order of their expected occurrence
	// parse handler for table descriptor
	CParseHandlerBase *pphTD = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenTableDescr), m_pphm, this);
	m_pphm->ActivateParseHandler(pphTD);

	// parse handler for the bitmap access path
	CParseHandlerBase *pphBitmap = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pphBitmap);

	// parse handler for the recheck condition
	CParseHandlerBase *pphRecheckCond =
			CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarRecheckCondFilter), m_pphm, this);
	m_pphm->ActivateParseHandler(pphRecheckCond);

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
	this->Append(pphRecheckCond);
	this->Append(pphBitmap);
	this->Append(pphTD);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerPhysicalAbstractBitmapScan::EndElementHelper
//
//	@doc:
//		Common EndElement functionality for children of this class
//
//---------------------------------------------------------------------------
void
CParseHandlerPhysicalAbstractBitmapScan::EndElementHelper
	(
	const XMLCh* const xmlszLocalname,
	Edxltoken edxltoken,
	ULONG ulPartIndexId,
	ULONG ulPartIndexIdPrintable
	)
{
	if (0 != XMLString::compareString
				(
				CDXLTokens::XmlstrToken(edxltoken),
				xmlszLocalname
				))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct nodes from the created child nodes
	CParseHandlerProperties *pphProp = dynamic_cast<CParseHandlerProperties *>((*this)[0]);
	CParseHandlerProjList *pphPrL = dynamic_cast<CParseHandlerProjList*>((*this)[1]);
	CParseHandlerFilter *pphFilter = dynamic_cast<CParseHandlerFilter *>((*this)[2]);
	CParseHandlerFilter *pphRecheckCond = dynamic_cast<CParseHandlerFilter *>((*this)[3]);
	CParseHandlerScalarOp *pphBitmap =
			dynamic_cast<CParseHandlerScalarOp*>((*this)[4]);
	CParseHandlerTableDescr *pphTD = dynamic_cast<CParseHandlerTableDescr*>((*this)[5]);

	GPOS_ASSERT(NULL != pphTD->Pdxltabdesc());

	// set table descriptor
	CDXLTableDescr *pdxltabdesc = pphTD->Pdxltabdesc();
	pdxltabdesc->AddRef();
	CDXLPhysical *pdxlop = NULL;

	if (EdxltokenPhysicalBitmapTableScan == edxltoken)
	{
		pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalBitmapTableScan(m_pmp, pdxltabdesc);
	}
	else
	{
		GPOS_ASSERT(EdxltokenPhysicalDynamicBitmapTableScan == edxltoken);
		pdxlop = GPOS_NEW(m_pmp) CDXLPhysicalDynamicBitmapTableScan(m_pmp, pdxltabdesc, ulPartIndexId, ulPartIndexIdPrintable);
	}
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	// set statictics and physical properties
	CParseHandlerUtils::SetProperties(m_pdxln, pphProp);

	// add constructed children
	AddChildFromParseHandler(pphPrL);
	AddChildFromParseHandler(pphFilter);
	AddChildFromParseHandler(pphRecheckCond);

	AddChildFromParseHandler(pphBitmap);

#ifdef GPOS_DEBUG
	pdxlop->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
