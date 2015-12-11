//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerLogicalSetOp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		set operators.
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalSetOp.h"
#include "naucrates/dxl/parser/CParseHandlerColDescr.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSetOp::CParseHandlerLogicalSetOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalSetOp::CParseHandlerLogicalSetOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot),
	m_edxlsetop(EdxlsetopSentinel),
	m_pdrgpdrgpulInputColIds(NULL),
	m_fCastAcrossInputs(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSetOp::~CParseHandlerLogicalSetOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalSetOp::~CParseHandlerLogicalSetOp()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSetOp::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalSetOp::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{

	if (0 == this->UlLength())
	{
		m_edxlsetop = Edxlsetop(xmlszLocalname);

		if (EdxlsetopSentinel == m_edxlsetop)
		{
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiDXLUnexpectedTag,
				CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz()
				);
		}

		// parse array of input colid arrays
		const XMLCh *xmlszInputColIds = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenInputCols));
		m_pdrgpdrgpulInputColIds = CDXLOperatorFactory::PdrgpdrgpulFromXMLCh(m_pphm->Pmm(), xmlszInputColIds, EdxltokenInputCols, EdxltokenLogicalSetOperation);

		// install column descriptor parsers
		CParseHandlerBase *pphColDescr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenColumns), m_pphm, this);
		m_pphm->ActivateParseHandler(pphColDescr);

		m_fCastAcrossInputs = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenCastAcrossInputs, EdxltokenLogicalSetOperation);

		// store child parse handler in array
		this->Append(pphColDescr);
	}
	else
	{
		// already have seen a set operation
		GPOS_ASSERT(EdxlsetopSentinel != m_edxlsetop);

		// create child node parsers
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogical), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		this->Append(pphChild);
		
		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSetOp::Edxlsetop
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
EdxlSetOpType
CParseHandlerLogicalSetOp::Edxlsetop
	(
	const XMLCh* const xmlszLocalname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalUnion), xmlszLocalname))
	{
		return EdxlsetopUnion;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalUnionAll), xmlszLocalname))
	{
		return EdxlsetopUnionAll;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalIntersect), xmlszLocalname))
	{
		return EdxlsetopIntersect;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalIntersectAll), xmlszLocalname))
	{
		return EdxlsetopIntersectAll;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalDifference), xmlszLocalname))
	{
		return EdxlsetopDifference;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalDifferenceAll), xmlszLocalname))
	{
		return EdxlsetopDifferenceAll;
	}

	return EdxlsetopSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalSetOp::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalSetOp::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	EdxlSetOpType edxlsetop = Edxlsetop(xmlszLocalname);

	if(EdxlsetopSentinel == edxlsetop && m_edxlsetop != edxlsetop)
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	const ULONG ulLen = this->UlLength();
	GPOS_ASSERT(3 <= ulLen);

	// get the columns descriptors
	CParseHandlerColDescr *pphColDescr = dynamic_cast<CParseHandlerColDescr *>((*this)[0]);
	GPOS_ASSERT(NULL != pphColDescr->Pdrgpdxlcd());
	DrgPdxlcd *pdrgpdxlcd = pphColDescr->Pdrgpdxlcd();

	pdrgpdxlcd->AddRef();
	CDXLLogicalSetOp *pdxlop = GPOS_NEW(m_pmp) CDXLLogicalSetOp(m_pmp, edxlsetop, pdrgpdxlcd, m_pdrgpdrgpulInputColIds, m_fCastAcrossInputs);
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

	for (ULONG ul = 1; ul < ulLen; ul++)
	{
		// add constructed logical children from child parse handlers
		CParseHandlerLogicalOp *pphChild = dynamic_cast<CParseHandlerLogicalOp*>((*this)[ul]);
		AddChildFromParseHandler(pphChild);
	}		
	
#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}
// EOF
