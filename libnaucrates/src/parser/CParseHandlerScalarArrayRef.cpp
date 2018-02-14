//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CParseHandlerScalarArrayRef.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing arrayref
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarArrayRef.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLScalarArrayRef.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRef::CParseHandlerScalarArrayRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarArrayRef::CParseHandlerScalarArrayRef
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_ulIndexLists(0),
	m_fParsingRefExpr(false),
	m_fParsingAssignExpr(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRef::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayRef::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRef), xmlszLocalname))
	{
		// initialize the arrayref node
		GPOS_ASSERT(NULL == m_pdxln);

		// parse types
		IMDId *pmdidElem = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenArrayElementType, EdxltokenScalarArrayRef);
		IMDId *pmdidArray = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenArrayType, EdxltokenScalarArrayRef);
		IMDId *pmdidReturn = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeId, EdxltokenScalarArrayRef);
		INT iTypeModifier = CDXLOperatorFactory::IValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenTypeMod, EdxltokenScalarArrayRef, true, IDefaultTypeModifier);

		m_pdxln = GPOS_NEW(m_pmp) CDXLNode (m_pmp, GPOS_NEW(m_pmp) CDXLScalarArrayRef(m_pmp, pmdidElem, iTypeModifier, pmdidArray, pmdidReturn));
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexList), xmlszLocalname))
	{
		GPOS_ASSERT(NULL != m_pdxln);
		GPOS_ASSERT(2 > m_ulIndexLists);

		// parse index list
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefIndexList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);
		m_ulIndexLists++;

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefExpr), xmlszLocalname))
	{
		GPOS_ASSERT(NULL != m_pdxln);
		GPOS_ASSERT(2 == m_ulIndexLists);
		GPOS_ASSERT(!m_fParsingRefExpr);
		GPOS_ASSERT(!m_fParsingAssignExpr);

		m_fParsingRefExpr = true;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefAssignExpr), xmlszLocalname))
	{
		GPOS_ASSERT(NULL != m_pdxln);
		GPOS_ASSERT(2 == m_ulIndexLists);
		GPOS_ASSERT(!m_fParsingRefExpr);
		GPOS_ASSERT(!m_fParsingAssignExpr);

		m_fParsingAssignExpr = true;
	}
	else
	{
		// parse scalar child
		GPOS_ASSERT(m_fParsingRefExpr || m_fParsingAssignExpr);

		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarArrayRef::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarArrayRef::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRef), xmlszLocalname))
	{
		// add constructed children from child parse handlers
		const ULONG ulSize = this->UlLength();
		GPOS_ASSERT(3 == ulSize || 4 == ulSize);

		for (ULONG ul = 0; ul < ulSize; ul++)
		{
			CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
			AddChildFromParseHandler(pphChild);
		}

		// deactivate handler
		m_pphm->DeactivateHandler();
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefExpr), xmlszLocalname))
	{
		GPOS_ASSERT(m_fParsingRefExpr);

		m_fParsingRefExpr = false;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarArrayRefAssignExpr), xmlszLocalname))
	{
		GPOS_ASSERT(m_fParsingAssignExpr);

		m_fParsingAssignExpr = false;
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

// EOF
