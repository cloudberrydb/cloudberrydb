//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBScalarOp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB scalar operators.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDGPDBScalarOp.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/md/CMDScalarOpGPDB.h"

using namespace gpdxl;
using namespace gpmd;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBScalarOp::CParseHandlerMDGPDBScalarOp
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMDGPDBScalarOp::CParseHandlerMDGPDBScalarOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidTypeLeft(NULL),
	m_pmdidTypeRight(NULL),
	m_pmdidTypeResult(NULL),
	m_pmdidFunc(NULL),
	m_pmdidOpCommute(NULL),
	m_pmdidOpInverse(NULL),
	m_ecmpt(IMDType::EcmptOther),
	m_fReturnsNullOnNullInput(false)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBScalarOp::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBScalarOp::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOp), xmlszLocalname))
	{
		// parse operator name
		const XMLCh *xmlszOpName = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenName,
															EdxltokenGPDBScalarOp
															);

		CWStringDynamic *pstrOpName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszOpName);
		
		// create a copy of the string in the CMDName constructor
		m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrOpName);
		
		GPOS_DELETE(pstrOpName);

		// parse metadata id info
		m_pmdid = CDXLOperatorFactory::PmdidFromAttrs
										(
										m_pphm->Pmm(),
										attrs,
										EdxltokenMdid,
										EdxltokenGPDBScalarOp
										);
		
		const XMLCh *xmlszCmpType = CDXLOperatorFactory::XmlstrFromAttrs
									(
									attrs,
									EdxltokenGPDBScalarOpCmpType,
									EdxltokenGPDBScalarOp
									);

		m_ecmpt = CDXLOperatorFactory::Ecmpt(xmlszCmpType);

		// null-returning property is optional
		const XMLCh *xmlszReturnsNullOnNullInput = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenReturnsNullOnNullInput));
		if (NULL != xmlszReturnsNullOnNullInput)
		{
			m_fReturnsNullOnNullInput = CDXLOperatorFactory::FValueFromAttrs
								(
								m_pphm->Pmm(),
								attrs,
								EdxltokenReturnsNullOnNullInput,
								EdxltokenGPDBScalarOp
								);
		}

	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpLeftTypeId), xmlszLocalname))
	{
		// parse left operand's type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeLeft = CDXLOperatorFactory::PmdidFromAttrs
												(
												m_pphm->Pmm(),
												attrs, 
												EdxltokenMdid,
												EdxltokenGPDBScalarOpLeftTypeId
												);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpRightTypeId), xmlszLocalname))
	{
		// parse right operand's type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeRight = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBScalarOpRightTypeId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpResultTypeId), xmlszLocalname))
	{
		// parse result type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeResult = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBScalarOpResultTypeId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpFuncId), xmlszLocalname))
	{
		// parse op func id
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidFunc = CDXLOperatorFactory::PmdidFromAttrs
												(
												m_pphm->Pmm(),
												attrs, 
												EdxltokenMdid,
												EdxltokenGPDBScalarOpFuncId
												);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpCommOpId), xmlszLocalname))
	{
		// parse commutator operator
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidOpCommute = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBScalarOpCommOpId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpInverseOpId), xmlszLocalname))
	{
		// parse inverse operator id
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidOpInverse = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBScalarOpInverseOpId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpClasses), xmlszLocalname))
	{
		// parse handler for operator class list
		CParseHandlerBase *pphOpClassList = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenMetadataIdList), m_pphm, this);
		m_pphm->ActivateParseHandler(pphOpClassList);
		this->Append(pphOpClassList);
		pphOpClassList->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
	}
	else
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBScalarOp::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBScalarOp::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOp), xmlszLocalname))
	{
		// construct the MD scalar operator object from its part
		GPOS_ASSERT(m_pmdid->FValid() && NULL != m_pmdname);
		
		GPOS_ASSERT(0 == this->UlLength() || 1 == this->UlLength());
		
		DrgPmdid *pdrgpmdidOpClasses = NULL;
		if (0 < this->UlLength())
		{
			CParseHandlerMetadataIdList *pphMdidOpClasses = dynamic_cast<CParseHandlerMetadataIdList*>((*this)[0]);
			pdrgpmdidOpClasses = pphMdidOpClasses->Pdrgpmdid();
			pdrgpmdidOpClasses->AddRef();
		}
		else 
		{
			pdrgpmdidOpClasses = GPOS_NEW(m_pmp) DrgPmdid(m_pmp);
		}
		m_pimdobj = GPOS_NEW(m_pmp) CMDScalarOpGPDB
				(
				m_pmp,
				m_pmdid,
				m_pmdname,
				m_pmdidTypeLeft,
				m_pmdidTypeRight,
				m_pmdidTypeResult,
				m_pmdidFunc,
				m_pmdidOpCommute,
				m_pmdidOpInverse,
				m_ecmpt,
				m_fReturnsNullOnNullInput,
				pdrgpmdidOpClasses
				)
				;
		
		// deactivate handler
		m_pphm->DeactivateHandler();

	}
	else if (!FSupportedChildElem(xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBScalarOp::FSupportedElem
//
//	@doc:
//		Is this a supported child elem of the scalar op
//
//---------------------------------------------------------------------------
BOOL
CParseHandlerMDGPDBScalarOp::FSupportedChildElem
	(
	const XMLCh* const xmlsz
	)
{
	return (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpLeftTypeId), xmlsz) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpRightTypeId), xmlsz) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpResultTypeId), xmlsz) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpFuncId), xmlsz) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpCommOpId), xmlsz) ||
			0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBScalarOpInverseOpId), xmlsz));
}

// EOF
