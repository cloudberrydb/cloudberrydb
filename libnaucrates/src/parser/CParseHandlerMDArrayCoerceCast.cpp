//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerMDArrayCoerceCast.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB array coerce cast functions
//---------------------------------------------------------------------------

#include "naucrates/md/CMDArrayCoerceCastGPDB.h"

#include "naucrates/dxl/parser/CParseHandlerMDArrayCoerceCast.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

// ctor
CParseHandlerMDArrayCoerceCast::CParseHandlerMDArrayCoerceCast
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot)
{}

// invoked by Xerces to process an opening tag
void
CParseHandlerMDArrayCoerceCast::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBArrayCoerceCast), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// parse func name
	const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenName,
														EdxltokenGPDBArrayCoerceCast
														);

	CMDName *pmdname = CDXLUtils::PmdnameFromXmlsz(m_pphm->Pmm(), xmlszFuncName);

	// parse cast properties
	IMDId *pmdid = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenMdid,
									EdxltokenGPDBArrayCoerceCast
									);

	IMDId *pmdidSrc = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastSrcType,
									EdxltokenGPDBArrayCoerceCast
									);

	IMDId *pmdidDest = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastDestType,
									EdxltokenGPDBArrayCoerceCast
									);

	IMDId *pmdidCastFunc = CDXLOperatorFactory::PmdidFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastFuncId,
									EdxltokenGPDBArrayCoerceCast
									);

	// parse whether func returns a set
	BOOL fBinaryCoercible = CDXLOperatorFactory::FValueFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenGPDBCastBinaryCoercible,
									EdxltokenGPDBArrayCoerceCast
									);

	// parse coercion path type
	IMDCast::EmdCoercepathType eCoercePathType = (IMDCast::EmdCoercepathType)
													CDXLOperatorFactory::IValueFromAttrs
															(
															m_pphm->Pmm(),
															attrs,
															EdxltokenGPDBCastCoercePathType,
															EdxltokenGPDBArrayCoerceCast
															);

	INT iTypeModifier = CDXLOperatorFactory::IValueFromAttrs
							(
							m_pphm->Pmm(),
							attrs,
							EdxltokenTypeMod,
							EdxltokenGPDBArrayCoerceCast,
							true,
							IDefaultTypeModifier
							);

	BOOL fIsExplicit =CDXLOperatorFactory::FValueFromAttrs
									(
									m_pphm->Pmm(),
									attrs,
									EdxltokenIsExplicit,
									EdxltokenGPDBArrayCoerceCast
									);

	EdxlCoercionForm edcf = (EdxlCoercionForm) CDXLOperatorFactory::IValueFromAttrs
																		(
																		m_pphm->Pmm(),
																		attrs,
																		EdxltokenCoercionForm,
																		EdxltokenGPDBArrayCoerceCast
																		);

	INT iLoc = CDXLOperatorFactory::IValueFromAttrs
							(
							m_pphm->Pmm(),
							attrs,
							EdxltokenLocation,
							EdxltokenGPDBArrayCoerceCast
							);

	m_pimdobj = GPOS_NEW(m_pmp) CMDArrayCoerceCastGPDB(m_pmp, pmdid, pmdname, pmdidSrc, pmdidDest, fBinaryCoercible, pmdidCastFunc, eCoercePathType, iTypeModifier, fIsExplicit, edcf, iLoc);
}

// invoked by Xerces to process a closing tag
void
CParseHandlerMDArrayCoerceCast::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBArrayCoerceCast), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// deactivate handler
	m_pphm->DeactivateHandler();
}

// EOF
