//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBCheckConstraint.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing an MD check constraint
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDCheckConstraintGPDB.h"

#include "naucrates/dxl/parser/CParseHandlerMDGPDBCheckConstraint.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;
using namespace gpmd;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBCheckConstraint::CParseHandlerMDGPDBCheckConstraint
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDGPDBCheckConstraint::CParseHandlerMDGPDBCheckConstraint
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidRel(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBCheckConstraint::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBCheckConstraint::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname,
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraint), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// new md object
	GPOS_ASSERT(NULL == m_pmdid);

	// parse mdid
	m_pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenCheckConstraint);

	// parse check constraint name
	const XMLCh *xmlszColName = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenName, EdxltokenCheckConstraint);
	CWStringDynamic *pstrColName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszColName);

	// create a copy of the string in the CMDName constructor
	m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrColName);
	GPOS_DELETE(pstrColName);

	// parse mdid of relation
	m_pmdidRel = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelationMdid, EdxltokenCheckConstraint);

	// create and activate the parse handler for the child scalar expression node
	CParseHandlerBase *pph = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenScalar), m_pphm, this);
	m_pphm->ActivateParseHandler(pph);

	// store parse handler
	this->Append(pph);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBCheckConstraint::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBCheckConstraint::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCheckConstraint), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// get node for default value expression from child parse handler
	CParseHandlerScalarOp *pphOp = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);

	CDXLNode *pdxlnScExpr = pphOp->Pdxln();
	GPOS_ASSERT(NULL != pdxlnScExpr);
	pdxlnScExpr->AddRef();

	m_pimdobj = GPOS_NEW(m_pmp) CMDCheckConstraintGPDB(m_pmp, m_pmdid, m_pmdname, m_pmdidRel, pdxlnScExpr);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
