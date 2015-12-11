//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerScalarWindowRef.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar WindowRef
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
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/parser/CParseHandlerScalarWindowRef.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarWindowRef::CParseHandlerScalarWindowRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerScalarWindowRef::CParseHandlerScalarWindowRef
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
//		CParseHandlerScalarWindowRef::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarWindowRef::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarWindowref), xmlszLocalname))
	{
		// parse and create scalar WindowRef (window function)
		CDXLScalarWindowRef *pdxlop =
				(CDXLScalarWindowRef*) CDXLOperatorFactory::PdxlopWindowRef(m_pphm->Pmm(), attrs);

		// construct node from the created scalar window ref
		m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
	}
	else
	{
		// we must have seen an window ref already and initialized the window ref node
		GPOS_ASSERT(NULL != m_pdxln);

		CParseHandlerBase *pphOp =
				CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphOp);

		// store parse handlers
		this->Append(pphOp);

		pphOp->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarWindowRef::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarWindowRef::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarWindowref), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	const ULONG ulSize = this->UlLength();
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarOp *pphOp = dynamic_cast<CParseHandlerScalarOp *>((*this)[ul]);
		AddChildFromParseHandler(pphOp);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
