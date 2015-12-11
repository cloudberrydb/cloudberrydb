//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerTraceFlags.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing trace flags
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/common/CBitSet.h"

#include "naucrates/dxl/parser/CParseHandlerTraceFlags.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/traceflags/traceflags.h"

#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTraceFlags::CParseHandlerTraceFlags
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerTraceFlags::CParseHandlerTraceFlags
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pbs(NULL)
{
	m_pbs = GPOS_NEW(pmp) CBitSet(pmp, EopttraceSentinel);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTraceFlags::~CParseHandlerTraceFlags
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerTraceFlags::~CParseHandlerTraceFlags()
{
	m_pbs->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTraceFlags::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTraceFlags::StartElement
	(
	const XMLCh* const , //xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const , //xmlszQname,
	const Attributes& attrs
	)
{	
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTraceFlags), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse and tokenize traceflags
	const XMLCh *xmlszTraceFlags = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenValue,
															EdxltokenTraceFlags
															);
	
	DrgPul *pdrgpul = CDXLOperatorFactory::PdrgpulFromXMLCh
												(
												m_pphm->Pmm(),
												xmlszTraceFlags, 
												EdxltokenDistrColumns,
												EdxltokenRelation
												);
	
	for (ULONG ul = 0; ul < pdrgpul->UlLength(); ul++)
	{
		ULONG *pul = (*pdrgpul)[ul];
		m_pbs->FExchangeSet(*pul);
	}
	
	pdrgpul->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTraceFlags::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerTraceFlags::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTraceFlags), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTraceFlags::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerTraceFlags::Edxlphtype() const
{
	return EdxlphTraceFlags;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerTraceFlags::Pbs
//
//	@doc:
//		Returns the bitset for the trace flags
//
//---------------------------------------------------------------------------
CBitSet *
CParseHandlerTraceFlags::Pbs()
{
	return m_pbs;
}
// EOF
