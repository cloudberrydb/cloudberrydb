//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDType.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing metadata for
//		GPDB types.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMDType.h"

#include "naucrates/dxl/gpdb_types.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeOidGPDB.h"

using namespace gpdxl;
using namespace gpmd;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::CParseHandlerMDType
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMDType::CParseHandlerMDType
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerMetadataObject(pmp, pphm, pphRoot),
	m_pmdid(NULL),
	m_pmdname(NULL),
	m_pmdidOpEq(NULL),
	m_pmdidOpNEq(NULL),
	m_pmdidOpLT(NULL),
	m_pmdidOpLEq(NULL),
	m_pmdidOpGT(NULL),
	m_pmdidOpGEq(NULL),
	m_pmdidOpComp(NULL),
	m_pmdidMin(NULL),
	m_pmdidMax(NULL),
	m_pmdidAvg(NULL),
	m_pmdidSum(NULL),
	m_pmdidCount(NULL),
	m_fHashable(false),
	m_fComposite(false),
	m_pmdidBaseRelation(NULL),
	m_pmdidTypeArray(NULL)
{
	// default: no aggregates for type
	m_pmdidMin = GPOS_NEW(pmp) CMDIdGPDB(0);
	m_pmdidMax = GPOS_NEW(pmp) CMDIdGPDB(0);
	m_pmdidAvg = GPOS_NEW(pmp) CMDIdGPDB(0);
	m_pmdidSum = GPOS_NEW(pmp) CMDIdGPDB(0);
	m_pmdidCount = GPOS_NEW(pmp) CMDIdGPDB(0);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::~CParseHandlerMDType
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerMDType::~CParseHandlerMDType()
{
	m_pmdid->Release();
	m_pmdidOpEq->Release();
	m_pmdidOpNEq->Release();
	m_pmdidOpLT->Release();
	m_pmdidOpLEq->Release();
	m_pmdidOpGT->Release();
	m_pmdidOpGEq->Release();
	m_pmdidOpComp->Release();
	m_pmdidTypeArray->Release();
	m_pmdidMin->Release();
	m_pmdidMax->Release();
	m_pmdidAvg->Release();
	m_pmdidSum->Release();
	m_pmdidCount->Release();
	CRefCount::SafeRelease(m_pmdidBaseRelation);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDType::StartElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const, // xmlszQname
	const Attributes& attrs
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenMDType), xmlszLocalname))
	{
		// parse metadata id info
		m_pmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, EdxltokenMDType);
		
		if (!FBuiltInType(m_pmdid))
		{
			// parse type name
			const XMLCh *xmlszTypeName = CDXLOperatorFactory::XmlstrFromAttrs
																(
																attrs,
																EdxltokenName,
																EdxltokenMDType
																);

			CWStringDynamic *pstrTypeName = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszTypeName);

			// create a copy of the string in the CMDName constructor
			m_pmdname = GPOS_NEW(m_pmp) CMDName(m_pmp, pstrTypeName);
			GPOS_DELETE(pstrTypeName);
			
			// parse if type is redistributable
			m_fRedistributable = CDXLOperatorFactory::FValueFromAttrs
														(
														m_pphm->Pmm(),
														attrs,
														EdxltokenMDTypeRedistributable,
														EdxltokenMDType
														);

			// parse if type is passed by value
			m_fByValue = CDXLOperatorFactory::FValueFromAttrs
												(
												m_pphm->Pmm(),
												attrs,
												EdxltokenMDTypeByValue,
												EdxltokenMDType
												);
			
			// parse if type is hashable
			m_fHashable = CDXLOperatorFactory::FValueFromAttrs
										(
										m_pphm->Pmm(),
										attrs,
										EdxltokenMDTypeHashable,
										EdxltokenMDType
										);

			// parse if type is composite
			const XMLCh *xmlszAttributeVal = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenMDTypeComposite));
			if (NULL == xmlszAttributeVal)
			{
				m_fComposite = false;
			}
			else
			{
				m_fComposite = CDXLOperatorFactory::FValueFromXmlstr
										(
										m_pphm->Pmm(),
										xmlszAttributeVal,
										EdxltokenMDTypeComposite,
										EdxltokenMDType
										);
			}

			if (m_fComposite)
			{
				// get base relation id
				m_pmdidBaseRelation = CDXLOperatorFactory::PmdidFromAttrs
										(
										m_pphm->Pmm(),
										attrs,
										EdxltokenMDTypeRelid,
										EdxltokenMDType
										);
			}

			// parse if type is fixed-length			
			m_fFixedLength = CDXLOperatorFactory::FValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenMDTypeFixedLength,
											EdxltokenMDType
											);
			if (m_fFixedLength)
			{
				// get type length
				m_iLength = CDXLOperatorFactory::IValueFromAttrs
											(
											m_pphm->Pmm(),
											attrs,
											EdxltokenMDTypeLength,
											EdxltokenMDType
											);
			}
		}
	}
	else
	{
		ParseMdid(xmlszLocalname, attrs);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::ParseMdid
//
//	@doc:
//		Parse the mdid
//
//---------------------------------------------------------------------------
void
CParseHandlerMDType::ParseMdid
	(
	const XMLCh *xmlszLocalname, 	
	const Attributes& attrs
	)
{
	const SMdidMapElem rgmdidmap[] =
	{
		{EdxltokenMDTypeEqOp, &m_pmdidOpEq},
		{EdxltokenMDTypeNEqOp, &m_pmdidOpNEq},
		{EdxltokenMDTypeLTOp, &m_pmdidOpLT},
		{EdxltokenMDTypeLEqOp, &m_pmdidOpLEq},
		{EdxltokenMDTypeGTOp, &m_pmdidOpGT},
		{EdxltokenMDTypeGEqOp, &m_pmdidOpGEq},
		{EdxltokenMDTypeCompOp, &m_pmdidOpComp},
		{EdxltokenMDTypeArray, &m_pmdidTypeArray},
		{EdxltokenMDTypeAggMin, &m_pmdidMin},
		{EdxltokenMDTypeAggMax, &m_pmdidMax},
		{EdxltokenMDTypeAggAvg, &m_pmdidAvg},
		{EdxltokenMDTypeAggSum, &m_pmdidSum},
		{EdxltokenMDTypeAggCount, &m_pmdidCount},
	};
	
	Edxltoken edxltoken = EdxltokenSentinel;
	IMDId **ppmdid = NULL;
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgmdidmap); ul++)
	{
		SMdidMapElem mdidmapelem = rgmdidmap[ul];
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(mdidmapelem.m_edxltoken), xmlszLocalname))
		{
			edxltoken = mdidmapelem.m_edxltoken;
			ppmdid = mdidmapelem.m_ppmdid;
		}
	}

	GPOS_ASSERT(EdxltokenSentinel != edxltoken);
	GPOS_ASSERT(NULL != ppmdid);

	if (m_pmdidMin == *ppmdid || m_pmdidMax == *ppmdid || m_pmdidAvg == *ppmdid  || 
		m_pmdidSum == *ppmdid || m_pmdidCount == *ppmdid)
	{
		(*ppmdid)->Release();
	}
	
	*ppmdid = CDXLOperatorFactory::PmdidFromAttrs(m_pphm->Pmm(), attrs, EdxltokenMdid, edxltoken);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::FBuiltInType
//
//	@doc:
//		Is this a built-in type
//
//---------------------------------------------------------------------------
BOOL
CParseHandlerMDType::FBuiltInType
	(
	const IMDId *pmdid
	)
	const
{
	if (IMDId::EmdidGPDB != pmdid->Emdidt())
	{
		return false;
	}
	
	const CMDIdGPDB *pmdidGPDB = CMDIdGPDB::PmdidConvert(pmdid);
	
	switch (pmdidGPDB->OidObjectId())
	{
		case GPDB_INT2:
		case GPDB_INT4:
		case GPDB_INT8:
		case GPDB_BOOL:
		case GPDB_OID:
			return true;
		default:
			return false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::Ppmdid
//
//	@doc:
//		Return the address of the mdid variable corresponding to the dxl token
//
//---------------------------------------------------------------------------
IMDId **
CParseHandlerMDType::Ppmdid
	(
	Edxltoken edxltoken
	)
{
	switch (edxltoken)
		{
			case EdxltokenMDTypeEqOp:
				return &m_pmdidOpEq;

			case EdxltokenMDTypeNEqOp:
				return &m_pmdidOpNEq;

			case EdxltokenMDTypeLTOp:
				return &m_pmdidOpLT;

			case EdxltokenMDTypeLEqOp:
				return &m_pmdidOpLEq;

			case EdxltokenMDTypeGTOp:
				return &m_pmdidOpGT;

			case EdxltokenMDTypeGEqOp:
				return &m_pmdidOpGEq;

			case EdxltokenMDTypeCompOp:
				return &m_pmdidOpComp;

			case EdxltokenMDTypeArray:
				return &m_pmdidTypeArray;

			default:
				break;
		}
	GPOS_ASSERT(!"Unexpected DXL token when parsing MDType");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMDType::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerMDType::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenMDType), xmlszLocalname))
	{
		// construct the MD type object from its part
		GPOS_ASSERT(m_pmdid->FValid());

		// TODO:  - Jan 30, 2012; add support for other types of mdids
		
		const CMDIdGPDB *pmdidGPDB = CMDIdGPDB::PmdidConvert(m_pmdid);
		
		switch(pmdidGPDB->OidObjectId())
		{
			case GPDB_INT2:
				m_pimdobj = GPOS_NEW(m_pmp) CMDTypeInt2GPDB(m_pmp);
				break;

			case GPDB_INT4:
				m_pimdobj = GPOS_NEW(m_pmp) CMDTypeInt4GPDB(m_pmp);
				break;

			case GPDB_INT8:
				m_pimdobj = GPOS_NEW(m_pmp) CMDTypeInt8GPDB(m_pmp);
				break;

			case GPDB_BOOL:
				m_pimdobj = GPOS_NEW(m_pmp) CMDTypeBoolGPDB(m_pmp);
				break;

			case GPDB_OID:
				m_pimdobj = GPOS_NEW(m_pmp) CMDTypeOidGPDB(m_pmp);
				break;

			default:
				m_pmdid->AddRef();
				m_pmdidOpEq->AddRef();
				m_pmdidOpNEq->AddRef();
				m_pmdidOpLT->AddRef();
				m_pmdidOpLEq->AddRef();
				m_pmdidOpGT->AddRef();
				m_pmdidOpGEq->AddRef();
				m_pmdidOpComp->AddRef();
				m_pmdidMin->AddRef();
				m_pmdidMax->AddRef();
				m_pmdidAvg->AddRef();
				m_pmdidSum->AddRef();
				m_pmdidCount->AddRef();
				if(NULL != m_pmdidBaseRelation)
				{
					m_pmdidBaseRelation->AddRef();
				}
				m_pmdidTypeArray->AddRef();
				
				ULONG ulLen = 0;
				if (0 < m_iLength)
				{
					ulLen = (ULONG) m_iLength;
				}
				m_pimdobj = GPOS_NEW(m_pmp) CMDTypeGenericGPDB
										(
										m_pmp,
										m_pmdid,
										m_pmdname,
										m_fRedistributable,
										m_fFixedLength,
										ulLen,
										m_fByValue,
										m_pmdidOpEq,
										m_pmdidOpNEq,
										m_pmdidOpLT,
										m_pmdidOpLEq,
										m_pmdidOpGT,
										m_pmdidOpGEq,
										m_pmdidOpComp,
										m_pmdidMin,
										m_pmdidMax,
										m_pmdidAvg,
										m_pmdidSum,
										m_pmdidCount,
										m_fHashable,
										m_fComposite,
										m_pmdidBaseRelation,
										m_pmdidTypeArray,
										m_iLength
										);
				break;
		}
		
		// deactivate handler
		m_pphm->DeactivateHandler();
	}
}

// EOF
