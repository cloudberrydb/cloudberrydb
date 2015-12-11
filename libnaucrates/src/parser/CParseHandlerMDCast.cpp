//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerMDCast.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB cast functions
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDCastGPDB.h"

#include "naucrates/dxl/parser/CParseHandlerMDCast.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDCast::CParseHandlerMDCast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDCast::CParseHandlerMDCast
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
//		CParseHandlerMDCast::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDCast::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBCast), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	// parse func name
	const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenName,
														EdxltokenGPDBCast
														);

	CMDName *pmdname = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszFuncName);


	// parse cast properties
	IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenMdid,
									EdxltokenGPDBCast
									);
	
	IMDId *pmdidSrc = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastSrcType,
									EdxltokenGPDBCast
									);
	
	IMDId *pmdidDest = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastDestType,
									EdxltokenGPDBCast
									);
	
	IMDId *pmdidCastFunc = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastFuncId,
									EdxltokenGPDBCast
									);
		
	// parse whether func returns a set
	BOOL fBinaryCoercible = CDXLOperatorFactory::FValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenGPDBCastBinaryCoercible,
											EdxltokenGPDBCast
											);
	
	m_pimdobj = GPOS_NEW(m_pmp) CMDCastGPDB(m_pmp, pmdid, pmdname, pmdidSrc, pmdidDest, fBinaryCoercible, pmdidCastFunc);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDCast::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDCast::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBCast), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
		
	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
