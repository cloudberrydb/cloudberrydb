//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarNullTest.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar NullTest.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarNullTest.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarNullTest::CParseHandlerScalarNullTest
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarNullTest::CParseHandlerScalarNullTest
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarNullTest::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarNullTest::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& // attrs
	)
{
	if ((0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIsNull), xmlszLocalname)) ||
		(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIsNotNull), xmlszLocalname)))
	{

		if( NULL != m_pdxln)
		{
			CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
		}

		BOOL fIsNull = true;

		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIsNotNull), xmlszLocalname))
		{
			fIsNull = false;
		}

		// parse and create scalar NullTest
		CDXLScalarNullTest *pdxlop = (CDXLScalarNullTest*) CDXLOperatorFactory::PdxlopNullTest(m_pphm->Pmm(), fIsNull);

		// construct node from the created child node
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);

		// parse handler for child scalar node
		CParseHandlerBase *pphChild = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphChild);

		// store parse handler
		this->Append(pphChild);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarNullTest::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarNullTest::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if ((0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIsNull), xmlszLocalname)) &&
		(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarIsNotNull), xmlszLocalname)))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(1 == this->UlLength());


	// add constructed child from child parse handler
	CParseHandlerScalarOp *pphChild = dynamic_cast<CParseHandlerScalarOp*>((*this)[0]);
	AddChildFromParseHandler(pphChild);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
