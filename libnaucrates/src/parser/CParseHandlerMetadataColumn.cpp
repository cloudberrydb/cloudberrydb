//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataColumn.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing column metadata.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMetadataColumn.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataColumns.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataIdList.h"
#include "naucrates/dxl/parser/CParseHandlerScalarOp.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumn::CParseHandlerMetadataColumn
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataColumn::CParseHandlerMetadataColumn
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pmdcol(NULL),
	m_pmdname(NULL),
	m_pmdidType(NULL),
	m_pdxlnDefaultValue(NULL),
	m_ulWidth(gpos::ulong_max)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumn::~CParseHandlerMetadataColumn
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataColumn::~CParseHandlerMetadataColumn()
{
	CRefCount::SafeRelease(m_pmdcol);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumn::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMetadataColumn::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumn), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse column name
	const XMLCh *xmlszColumnName = CDXLOperatorFactory::XmlstrFromAttrs
												(
												attrs,
												EdxltokenName,
												EdxltokenMetadataColumn
												);

	CWStringDynamic *pstrColumnName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszColumnName);
	
	// create a copy of the string in the CMDName constructor
	m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrColumnName);
	
	GPOS_DELETE(pstrColumnName);
	
	// parse attribute number
	m_iAttNo = CDXLOperatorFactory::IValueFromAttrs
								(
								m_pphm->Pmm(),
								attrs,
								EdxltokenAttno,
								EdxltokenMetadataColumn
								);

	m_pmdidType = CDXLOperatorFactory::PmdidFromAttrs
								(
								m_pphm->Pmm(),
								attrs,
								EdxltokenMdid,
								EdxltokenMetadataColumn
								);

	// parse optional type modifier
	m_iTypeModifier = CDXLOperatorFactory::IValueFromAttrs
								(
								m_pphm->Pmm(),
								attrs,
								EdxltokenTypeMod,
								EdxltokenColDescr,
								true,
								IDefaultTypeModifier
								);

	// parse attribute number
	m_fNullable = CDXLOperatorFactory::FValueFromAttrs
								(
								m_pphm->Pmm(),
								attrs,
								EdxltokenColumnNullable,
								EdxltokenMetadataColumn
								);

	// parse column length from attributes
	const XMLCh *xmlszColumnLength =  attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColWidth));

	if (NULL != xmlszColumnLength)
	{
		m_ulWidth = CDXLOperatorFactory::UlValueFromXmlstr
						(
						m_pphm->Pmm(),
						xmlszColumnLength,
						EdxltokenColWidth,
						EdxltokenColDescr
						);
	}

	m_fDropped = false;
	const XMLCh *xmlszDropped = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColDropped));

	if (NULL != xmlszDropped)
	{
		m_fDropped = CDXLOperatorFactory::FValueFromXmlstr
						(
						m_pphm->Pmm(),
						xmlszDropped,
						EdxltokenColDropped,
						EdxltokenMetadataColumn
						);
	}
	
	// install a parse handler for the default value
	CParseHandlerBase *pph = CParseHandlerFactory::Pph
										(
										m_pmp,
										CDXLTokens::XmlstrToken(EdxltokenColumnDefaultValue),
										m_pphm,
										this
										);
		
	// activate and store parse handler
	m_pphm->ActivateParseHandler(pph);
	this->Append(pph);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumn::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMetadataColumn::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if(0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenColumn), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	GPOS_ASSERT(1 == this->UlLength());
	
	// get node for default value expression from child parse handler
	CParseHandlerScalarOp *pphOp = dynamic_cast<CParseHandlerScalarOp *>((*this)[0]);
	
	m_pdxlnDefaultValue = pphOp->Pdxln();
	
	if (NULL != m_pdxlnDefaultValue)
	{
		m_pdxlnDefaultValue->AddRef();
	}
	
	m_pmdcol = GPOS_NEW(m_pmp) CMDColumn
							(
							m_pmdname,
							m_iAttNo,
							m_pmdidType,
							m_iTypeModifier,
							m_fNullable,
							m_fDropped,
							m_pdxlnDefaultValue,
							m_ulWidth
							);

	// deactivate handler
	m_pphm->DeactivateHandler();
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataColumn::Pmdcol
//
//	@doc:
//		Return the constructed list of metadata columns
//
//---------------------------------------------------------------------------
CMDColumn *
CParseHandlerMetadataColumn::Pmdcol()
{
	return m_pmdcol;
}

// EOF
