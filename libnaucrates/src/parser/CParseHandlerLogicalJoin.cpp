//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerLogicalJoin.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing logical
//		join operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerLogicalJoin.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalJoin::CParseHandlerLogicalJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalJoin::CParseHandlerLogicalJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerLogicalOp(pmp, pphm, pphRoot)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalJoin::~CParseHandlerLogicalJoin
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerLogicalJoin::~CParseHandlerLogicalJoin()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalJoin::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalJoin::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalJoin), xmlszLocalname))
	{
		if(NULL == m_pdxln)
		{
			// parse and create logical join operator
			CDXLLogicalJoin *pdxlopJoin = (CDXLLogicalJoin*) CDXLOperatorFactory::PdxlopLogicalJoin(m_pphm->Pmm(), attrs);

			// construct node from the created child nodes
			m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlopJoin);
		}
		else
		{
			// This is to support nested join.
			CParseHandlerBase *pphLgJoin = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenLogicalJoin), m_pphm, this);
			m_pphm->ActivateParseHandler(pphLgJoin);

			// store parse handlers
			this->Append(pphLgJoin);

			pphLgJoin->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		}
	}
	else
	{
		if(NULL == m_pdxln)
		{
			CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
		}

		// The child can either be a CDXLLogicalOp or CDXLScalar
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, xmlszLocalname, m_pphm, this);

		m_pphm->ActivateParseHandler(pphChild);

		// store parse handlers
		this->Append(pphChild);

		pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerLogicalJoin::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerLogicalJoin::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenLogicalJoin), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_pdxln );
	ULONG ulChildrenCount = this->UlLength();

	// Joins must atleast have 3 children (2 logical operators and 1 scalar operator)
	GPOS_ASSERT(2 < ulChildrenCount);

	// add constructed children from child parse handlers

	// Add the first n-1 logical operator from the first n-1 child parse handler
	for (ULONG ul = 0; ul < ulChildrenCount; ul++)
	{
		CParseHandlerOp *pphOp = dynamic_cast<CParseHandlerOp*>((*this)[ul]);
		GPOS_ASSERT(NULL != pphOp->Pdxln());
		AddChildFromParseHandler(pphOp);
	}

#ifdef GPOS_DEBUG
	m_pdxln->Pdxlop()->AssertValid(m_pdxln, false /* fValidateChildren */);
#endif // GPOS_DEBUG

	// deactivate handler
	m_pphm->DeactivateHandler();
}
// EOF
