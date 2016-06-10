//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBTrigger.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB triggers
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDGPDBTrigger.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#define GPMD_TRIGGER_ROW_BIT 0
#define GPMD_TRIGGER_BEFORE_BIT 1
#define GPMD_TRIGGER_INSERT_BIT 2
#define GPMD_TRIGGER_DELETE_BIT 3
#define GPMD_TRIGGER_UPDATE_BIT 4
#define GPMD_TRIGGER_BITMAP_LEN 5

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBTrigger::CParseHandlerMDGPDBTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDGPDBTrigger::CParseHandlerMDGPDBTrigger
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidRel(NULL),
	m_pmdidFunc(NULL),
	m_iType(0),
	m_fEnabled(false)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBTrigger::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBTrigger::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBTrigger), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	m_pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenGPDBTrigger);

	const XMLCh *xmlszName = CDXLOperatorFactory::XmlstrFromAttrs(attrs, EdxltokenName, EdxltokenGPDBTrigger);
	CWStringDynamic *pstrName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszName);
	m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrName);
	GPOS_DELETE(pstrName);
	GPOS_ASSERT(m_pmdid->FValid() && NULL != m_pmdname);

	m_pmdidRel = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenRelationMdid, EdxltokenGPDBTrigger);
	m_pmdidFunc = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenFuncId, EdxltokenGPDBTrigger);

	BOOL rgfProperties[GPMD_TRIGGER_BITMAP_LEN];
	rgfProperties[GPMD_TRIGGER_ROW_BIT] =
			CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGPDBTriggerRow, EdxltokenGPDBTrigger);
	rgfProperties[GPMD_TRIGGER_BEFORE_BIT] =
			CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGPDBTriggerBefore, EdxltokenGPDBTrigger);
	rgfProperties[GPMD_TRIGGER_INSERT_BIT] =
			CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGPDBTriggerInsert, EdxltokenGPDBTrigger);
	rgfProperties[GPMD_TRIGGER_DELETE_BIT] =
			CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGPDBTriggerDelete, EdxltokenGPDBTrigger);
	rgfProperties[GPMD_TRIGGER_UPDATE_BIT] =
			CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGPDBTriggerUpdate, EdxltokenGPDBTrigger);

	for (ULONG ul = 0; ul < GPMD_TRIGGER_BITMAP_LEN; ul++)
	{
		// if the current property flag is true then set the corresponding bit
		if (rgfProperties[ul])
		{
			m_iType |= (1 << ul);
		}
	}

	m_fEnabled = CDXLOperatorFactory::FValueFromAttrs(m_pphm->Pmm(), attrs, EdxltokenGPDBTriggerEnabled, EdxltokenGPDBTrigger);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBTrigger::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBTrigger::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBTrigger), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// construct the MD trigger object
	m_pimdobj = GPOS_NEW(m_pmp) CMDTriggerGPDB
								(
								m_pmp,
								m_pmdid,
								m_pmdname,
								m_pmdidRel,
								m_pmdidFunc,
								m_iType,
								m_fEnabled
								);

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
