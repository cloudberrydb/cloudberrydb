//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBFunc.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB functions.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDGPDBFunc.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"

#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"


using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::CParseHandlerMDGPDBFunc
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerMDGPDBFunc::CParseHandlerMDGPDBFunc
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidTypeResult(NULL),
	m_pdrgpmdidTypes(NULL),
	m_efuncstbl(CMDFunctionGPDB::EfsSentinel)
{}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBFunc::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFunc), xmlszLocalname))
	{
		// parse func name
		const XMLCh *xmlszFuncName = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenName,
															EdxltokenGPDBFunc
															);

		CWStringDynamic *pstrFuncName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszFuncName);
		
		// create a copy of the string in the CMDName constructor
		m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrFuncName);
		
		GPOS_DELETE(pstrFuncName);

		// parse metadata id info
		m_pmdid = CDXLOperatorFactory::PmdidFromAttrs
										(
										m_pphm->Pmm(),
										attrs,
										EdxltokenMdid,
										EdxltokenGPDBFunc
										);
		
		// parse whether func returns a set
		m_fReturnsSet = CDXLOperatorFactory::FValueFromAttrs
												(
												m_pphm->Pmm(),
												attrs,
												EdxltokenGPDBFuncReturnsSet,
												EdxltokenGPDBFunc
												);
		// parse whether func is strict
		m_fStrict = CDXLOperatorFactory::FValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenGPDBFuncStrict,
											EdxltokenGPDBFunc
											);
		
		// parse func stability property
		const XMLCh *xmlszStbl = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenGPDBFuncStability,
														EdxltokenGPDBFunc
														);
		
		m_efuncstbl = EFuncStability(xmlszStbl);

		// parse func data access property
		const XMLCh *xmlszDataAcc = CDXLOperatorFactory::XmlstrFromAttrs
														(
														attrs,
														EdxltokenGPDBFuncDataAccess,
														EdxltokenGPDBFunc
														);

		m_efuncdataacc = EFuncDataAccess(xmlszDataAcc);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncResultTypeId), xmlszLocalname))
	{
		// parse result type
		GPOS_ASSERT(NULL != m_pmdname);

		m_pmdidTypeResult = CDXLOperatorFactory::PmdidFromAttrs
													(
													m_pphm->Pmm(),
													attrs,
													EdxltokenMdid,
													EdxltokenGPDBFuncResultTypeId
													);
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOutputCols), xmlszLocalname))
	{
		// parse output column type
		GPOS_ASSERT(NULL != m_pmdname);
		GPOS_ASSERT(NULL == m_pdrgpmdidTypes);

		const XMLCh *xmlszTypes = CDXLOperatorFactory::XmlstrFromAttrs
															(
															attrs,
															EdxltokenTypeIds,
															EdxltokenOutputCols
															);

		m_pdrgpmdidTypes = CDXLOperatorFactory::PdrgpmdidFromXMLCh
													(
													m_pphm->Pmm(),
													xmlszTypes,
													EdxltokenTypeIds,
													EdxltokenOutputCols
													);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDGPDBFunc::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFunc), xmlszLocalname))
	{
		// construct the MD func object from its part
		GPOS_ASSERT(m_pmdid->FValid() && NULL != m_pmdname);
		
		m_pimdobj = GPOS_NEW(m_pmp) CMDFunctionGPDB(m_pmp,
												m_pmdid,
												m_pmdname,
												m_pmdidTypeResult,
												m_pdrgpmdidTypes,
												m_fReturnsSet,
												m_efuncstbl,
												m_efuncdataacc,
												m_fStrict);
		
		// deactivate handler
		m_pphm->DeactivateHandler();

	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncResultTypeId), xmlszLocalname) &&
			0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOutputCols), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::EFuncStability
//
//	@doc:
//		Parses function stability property from XML string
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncStbl 
CParseHandlerMDGPDBFunc::EFuncStability
	(
	const XMLCh *xmlsz
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncStable), xmlsz))
	{
		return CMDFunctionGPDB::EfsStable;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncImmutable), xmlsz))
	{
		return CMDFunctionGPDB::EfsImmutable;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncVolatile), xmlsz))
	{
		return CMDFunctionGPDB::EfsVolatile;
	}

	GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(EdxltokenGPDBFuncStability)->Wsz(),
		CDXLTokens::PstrToken(EdxltokenGPDBFunc)->Wsz()
		);

	return CMDFunctionGPDB::EfsSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDGPDBFunc::EFuncDataAccess
//
//	@doc:
//		Parses function data access property from XML string
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::EFuncDataAcc
CParseHandlerMDGPDBFunc::EFuncDataAccess
	(
	const XMLCh *xmlsz
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncNoSQL), xmlsz))
	{
		return CMDFunctionGPDB::EfdaNoSQL;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncContainsSQL), xmlsz))
	{
		return CMDFunctionGPDB::EfdaContainsSQL;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncReadsSQLData), xmlsz))
	{
		return CMDFunctionGPDB::EfdaReadsSQLData;
	}

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenGPDBFuncModifiesSQLData), xmlsz))
	{
		return CMDFunctionGPDB::EfdaModifiesSQLData;
	}

	GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(EdxltokenGPDBFuncDataAccess)->Wsz(),
		CDXLTokens::PstrToken(EdxltokenGPDBFunc)->Wsz()
		);

	return CMDFunctionGPDB::EfdaSentinel;
}

// EOF
