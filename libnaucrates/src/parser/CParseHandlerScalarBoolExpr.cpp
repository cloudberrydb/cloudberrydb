//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerScalarBoolExpr.cpp
//
//	@doc:
//		
//		Implementation of the SAX parse handler class for parsing scalar BoolExpr.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/dxl/parser/CParseHandlerScalarBoolExpr.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBoolExpr::CParseHandlerScalarBoolExpr
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerScalarBoolExpr::CParseHandlerScalarBoolExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerScalarOp(pmp, pphm, pphRoot),
	m_edxlBoolType(Edxland)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBoolExpr::StartElement
//
//	@doc:
//		Processes a Xerces start element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBoolExpr::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if ((0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolAnd), xmlszLocalname)) ||
		(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolOr), xmlszLocalname)) ||
		(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolNot), xmlszLocalname)))
	{
		if (NULL == m_pdxln)
		{
			if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolNot), xmlszLocalname))
			{
				m_edxlBoolType = Edxlnot;
			}
			else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolOr), xmlszLocalname))
			{
				m_edxlBoolType = Edxlor;
			}

			// parse and create scalar BoolExpr
			CDXLScalarBoolExpr *pdxlop = (CDXLScalarBoolExpr*) CDXLOperatorFactory::PdxlopBoolExpr(m_pphm->Pmm(), m_edxlBoolType);

			// construct node from the created child nodes
			m_pdxln = GPOS_NEW(m_pmp) CDXLNode(m_pmp, pdxlop);
		}
		else
		{

			// This is to support nested BoolExpr. TODO:  - create a separate xml tag for boolean expression
			CParseHandlerBase *pphBoolExpr = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalarBoolOr), m_pphm, this);
			m_pphm->ActivateParseHandler(pphBoolExpr);

			// store parse handlers
			this->Append(pphBoolExpr);

			pphBoolExpr->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		}
	}
	else
	{
		if(NULL == m_pdxln)
		{
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz());
		}

		CParseHandlerBase *pphOp = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
		m_pphm->ActivateParseHandler(pphOp);

		// store parse handlers
		this->Append(pphOp);

		pphOp->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBoolExpr::EndElement
//
//	@doc:
//		Processes a Xerces end element event
//
//---------------------------------------------------------------------------
void
CParseHandlerScalarBoolExpr::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	EdxlBoolExprType edxlBoolType =
			CParseHandlerScalarBoolExpr::EdxlBoolType(xmlszLocalname);

	if(EdxlBoolExprTypeSentinel == edxlBoolType || m_edxlBoolType != edxlBoolType)
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz());
	}

	const ULONG ulSize = this->UlLength();
	// If the operation is NOT then it only has one child.
	if (
	    ((((CDXLScalarBoolExpr*) m_pdxln->Pdxlop())->EdxlBoolType() == Edxlnot)
		&& (1 != ulSize))
		||
		((((CDXLScalarBoolExpr*) m_pdxln->Pdxlop())->EdxlBoolType() != Edxlnot)
		&& (2 > ulSize))
	  )
	{
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLIncorrectNumberOfChildren, CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname)->Wsz());
	}

	// add constructed children from child parse handlers
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		CParseHandlerScalarOp *pph = dynamic_cast<CParseHandlerScalarOp*>((*this)[ul]);
		AddChildFromParseHandler(pph);
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerScalarBoolExpr::EdxlBoolType
//
//	@doc:
//		Parse the bool type from the attribute value. Raise exception if it is invalid
//
//---------------------------------------------------------------------------
EdxlBoolExprType
CParseHandlerScalarBoolExpr::EdxlBoolType
	(
	const XMLCh *xmlszBoolType
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolNot), xmlszBoolType))
	{
		return Edxlnot;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolAnd), xmlszBoolType))
	{
		return Edxland;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenScalarBoolOr), xmlszBoolType))
	{
		return Edxlor;
	}

	return EdxlBoolExprTypeSentinel;
}

// EOF
