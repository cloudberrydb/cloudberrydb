//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarBooleanTest.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar BooleanTest.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarBooleanTest.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBooleanTest::CParseHandlerScalarBooleanTest
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarBooleanTest::CParseHandlerScalarBooleanTest
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_edxlBooleanTestType(EdxlbooleantestSentinel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBooleanTest::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBooleanTest::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	EdxlBooleanTestType edxlBooleanTestType =
			CParseHandlerScalarBooleanTest::EdxlBooleantestType(xmlszLocalname);

	if (EdxlbooleantestSentinel == edxlBooleanTestType)
	{
		if(NULL == m_pdxln)
		{
			GPOS_RAISE
				(
				gpdxl::ExmaDXL,
				gpdxl::ExmiDXLUnexpectedTag,
				CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz()
				);
		}
		else
		{
			CParseHandlerBase *pphChild =
					CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);

			m_pphm->ActivateParseHandler(pphChild);

			// store parse handlers
			this->Append(pphChild);

			pphChild->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		}

		return;
	}

	m_edxlBooleanTestType = edxlBooleanTestType;
	// parse and create scalar BooleanTest
	CDXLScalarBooleanTest *pdxlop =
			(CDXLScalarBooleanTest*) CDXLOperatorFactory::PdxlopBooleanTest(m_pphm->Pmm(), m_edxlBooleanTestType);

	// construct node from the created child nodes
	m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBooleanTest::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBooleanTest::EndElement
(
		const XMLCh* const, // xmlszUri,
		const XMLCh* const xmlszLocalname,
		const XMLCh* const // xmlszQname
)
{

	EdxlBooleanTestType edxlBooleanTestType = CParseHandlerScalarBooleanTest::EdxlBooleantestType(xmlszLocalname);

	if (EdxlbooleantestSentinel == edxlBooleanTestType )
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLUnexpectedTag,
			CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz()
			);
	}
	
	GPOS_ASSERT(edxlBooleanTestType == m_edxlBooleanTestType);
	GPOS_ASSERT(1 == this->UlLength());

	CParseHandlerScalarOp *pph = dynamic_cast<CParseHandlerScalarOp*>((*this)[0]);
	AddChildFromParseHandler(pph);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBooleanTest::EdxlBooleantestType
//
//	@doc:
//		Parse the boolean test type from the attribute value
//
//---------------------------------------------------------------------------
EdxlBooleanTestType
CParseHandlerScalarBooleanTest::EdxlBooleantestType
	(
	const XMLCh *xmlszBooleanTestType
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolTestIsTrue), xmlszBooleanTestType))
	{
		return EdxlbooleantestIsTrue;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolTestIsNotTrue), xmlszBooleanTestType))
	{
		return EdxlbooleantestIsNotTrue;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolTestIsFalse), xmlszBooleanTestType))
	{
		return EdxlbooleantestIsFalse;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolTestIsNotFalse), xmlszBooleanTestType))
	{
		return EdxlbooleantestIsNotFalse;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolTestIsUnknown), xmlszBooleanTestType))
	{
		return EdxlbooleantestIsUnknown;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolTestIsNotUnknown), xmlszBooleanTestType))
	{
		return EdxlbooleantestIsNotUnknown;
	}

	return EdxlbooleantestSentinel;
}

// EOF
