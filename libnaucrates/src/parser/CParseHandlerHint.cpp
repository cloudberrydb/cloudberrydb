//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerHint.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing hint
//		configuration
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerHint.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/xml/dxltokens.h"

#include "gpopt/engine/CHint.h"

using namespace gpdxl;
using namespace gpopt;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::CParseHandlerHint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerHint::CParseHandlerHint
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_phint(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::~CParseHandlerHint
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerHint::~CParseHandlerHint()
{
	CRefCount::SafeRelease(m_phint);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHint::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenHint), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse hint configuration options
	ULONG ulMinNumOfPartsToRequireSortOnInsert = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMinNumOfPartsToRequireSortOnInsert, EdxltokenHint);
	ULONG ulJoinArityForAssociativityCommutativity = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenJoinArityForAssociativityCommutativity, EdxltokenHint, true, gpos::int_max);
	ULONG ulArrayExpansionThreshold = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenArrayExpansionThreshold, EdxltokenHint, true, gpos::int_max);
	ULONG ulJoinOrderDPThreshold = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenJoinOrderDPThreshold, EdxltokenHint, true, JOIN_ORDER_DP_THRESHOLD);
	ULONG ulBroadcastThreshold = CDXLOperatorFactory::UlValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenBroadcastThreshold, EdxltokenHint, true, BROADCAST_THRESHOLD);
	ULONG fEnforceConstraintsOnDML = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenEnforceConstraintsOnDML, EdxltokenHint, true, true);

	m_phint = GPOS_NEW(m_pmp) CHint
								(
								ulMinNumOfPartsToRequireSortOnInsert,
								ulJoinArityForAssociativityCommutativity,
								ulArrayExpansionThreshold,
								ulJoinOrderDPThreshold,
								ulBroadcastThreshold,
								fEnforceConstraintsOnDML
								);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerHint::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenHint), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	GPOS_ASSERT(NULL != m_phint);
	GPOS_ASSERT(0 == this->UlLength());

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerHint::Edxlphtype() const
{
	return EdxlphHint;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerHint::Phint
//
//	@doc:
//		Returns the hint configuration
//
//---------------------------------------------------------------------------
CHint *
CParseHandlerHint::Phint() const
{
	return m_phint;
}

// EOF
