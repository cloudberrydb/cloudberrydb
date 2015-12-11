//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDScCmp.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB scalar comparison operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDScCmpGPDB.h"

#include "naucrates/dxl/parser/CParseHandlerMDScCmp.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDScCmp::CParseHandlerMDScCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDScCmp::CParseHandlerMDScCmp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDScCmp::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDScCmp::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBMDScCmp), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse operator name
	const XMLCh *xmlszOpName = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenName,
														EdxltokenGPDBMDScCmp
														);

	CMDName *pmdname = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszOpName);


	// parse scalar comparison properties
	IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenMdid,
									EdxltokenGPDBMDScCmp
									);
	
	IMDId *pmdidLeft = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBScalarOpLeftTypeId,
									EdxltokenGPDBMDScCmp
									);
	
	IMDId *pmdidRight = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBScalarOpRightTypeId,
									EdxltokenGPDBMDScCmp
									);
	
	IMDId *pmdidOp = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenOpNo,
									EdxltokenGPDBMDScCmp
									);
		
	// parse comparison type
	const XMLCh *xmlszCmpType = CDXLOperatorFactory::XmlstrFromAttrs
								(
								attrs,
								EdxltokenGPDBScalarOpCmpType,
								EdxltokenGPDBMDScCmp
								);

	IMDType::ECmpType ecmpt = CDXLOperatorFactory::Ecmpt(xmlszCmpType);
	
	m_pimdobj = GPOS_NEW(m_pmp) CMDScCmpGPDB(m_pmp, pmdid, pmdname, pmdidLeft, pmdidRight, ecmpt, pmdidOp);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDScCmp::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDScCmp::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBMDScCmp), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
