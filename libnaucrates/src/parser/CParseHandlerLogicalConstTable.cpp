//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalConstTable.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical 
//		const tables.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalConstTable.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerColDescr.h"

#include "naucrates/dxl/operators/CDXLLogicalConstTable.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalConstTable::CParseHandlerLogicalConstTable
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalConstTable::CParseHandlerLogicalConstTable
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot),
	m_pdrgpdrgpdxldatum(NULL),
	m_pdrgpdxldatum(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalConstTable::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalConstTable::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalConstTable), xmlszLocalname))
	{
		// start of a const table operator node
		GPOS_ASSERT(0 == this->UlLength());
		GPOS_ASSERT(NULL == m_pdrgpdrgpdxldatum);

		// initialize the array of const tuples (datum arrays)
		m_pdrgpdrgpdxldatum = GPOS_NEW(m_pmp) DrgPdrgPdxldatum(m_pmp);

		// install a parse handler for the columns
		CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumns), m_pphm, this);
		m_pphm->ActivateParseHandler(pphColDescr);
		
		// store parse handler
		this->Append(pphColDescr);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenConstTuple), xmlszLocalname))
	{
		GPOS_ASSERT(NULL != m_pdrgpdrgpdxldatum); // we must have already seen a logical const table
		GPOS_ASSERT(NULL == m_pdrgpdxldatum);

		// initialize the array of datums (const tuple)
		m_pdrgpdxldatum = GPOS_NEW(m_pmp) DrgPdxldatum(m_pmp);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenDatum), xmlszLocalname))
	{
		// we must have already seen a logical const table and a const tuple
		GPOS_ASSERT(NULL != m_pdrgpdrgpdxldatum);
		GPOS_ASSERT(NULL != m_pdrgpdxldatum);

		// translate the datum and add it to the datum array
		CDXLDatum *pdxldatum = CDXLOperatorFactory::Pdxldatum(m_pphm->Pmm(), attrs, EdxltokenScalarConstValue);
		m_pdrgpdxldatum->Append(pdxldatum);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalConstTable::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalConstTable::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalConstTable), xmlszLocalname))
	{
		GPOS_ASSERT(1 == this->UlLength());

		CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[0]);
		GPOS_ASSERT(NULL != pphColDescr->Pdrgpdxlcd());

		DrgPdxlcd *pdrgpdxlcd = pphColDescr->Pdrgpdxlcd();
		pdrgpdxlcd->AddRef();

		CDXLLogicalConstTable *pdxlopConstTable = GPOS_NEW(m_pmp) CDXLLogicalConstTable(m_pmp, pdrgpdxlcd, m_pdrgpdrgpdxldatum);
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopConstTable);

#ifdef GPOS_DEBUG
	pdxlopConstTable->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

		// deactivate handler
	  	m_pphm->DeactivateHandler();
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenConstTuple), xmlszLocalname))
	{
		GPOS_ASSERT(NULL != m_pdrgpdxldatum);
		m_pdrgpdrgpdxldatum->Append(m_pdrgpdxldatum);

		m_pdrgpdxldatum = NULL; // intialize for the parsing the next const tuple (if needed)
	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenDatum), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}
// EOF
