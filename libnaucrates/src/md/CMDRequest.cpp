//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDRequest.cpp
//
//	@doc:
//		Implementation of the class for metadata requests
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDRequest.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::CMDRequest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRequest::CMDRequest
	(
	IMemoryPool *pmp,
	DrgPmdid *pdrgpmdid,
	DrgPtr *pdrgptr
	)
	:
	m_pmp(pmp),
	m_pdrgpmdid(pdrgpmdid),
	m_pdrgptr(pdrgptr)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pdrgpmdid);
	GPOS_ASSERT(NULL != pdrgptr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::CMDRequest
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDRequest::CMDRequest
	(
	IMemoryPool *pmp,
	SMDTypeRequest *pmdtr
	)
	:
	m_pmp(pmp),
	m_pdrgpmdid(NULL),
	m_pdrgptr(NULL)
{
	GPOS_ASSERT(NULL != pmp);
	GPOS_ASSERT(NULL != pmdtr);
	
	m_pdrgpmdid = GPOS_NEW(m_pmp) DrgPmdid(m_pmp);
	m_pdrgptr = GPOS_NEW(m_pmp) DrgPtr(m_pmp);
	
	m_pdrgptr->Append(pmdtr);	
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::~CMDRequest
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDRequest::~CMDRequest()
{
	m_pdrgpmdid->Release();
	m_pdrgptr->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::Pstr
//
//	@doc:
//		Serialize system id
//
//---------------------------------------------------------------------------
CWStringDynamic *
CMDRequest::Pstr
	(
	CSystemId sysid
	) 
{
	CWStringDynamic *pstr = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);
	pstr->AppendFormat(GPOS_WSZ_LIT("%d.%ls"), sysid.Emdidt(), sysid.Wsz());
	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDRequest::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDRequest::Serialize
	(
	CXMLSerializer *pxmlser
	) 
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMDRequest));

	const ULONG ulMdids = m_pdrgpmdid->UlLength();
	for (ULONG ul = 0; ul < ulMdids; ul++)
	{
		IMDId *pmdid = (*m_pdrgpmdid)[ul];
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
										CDXLTokens::PstrToken(EdxltokenMdid));				
		pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenValue));
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenMdid));
	}

	const ULONG ulTypeRequests = m_pdrgptr->UlLength();
	for (ULONG ul = 0; ul < ulTypeRequests; ul++)
	{
		SMDTypeRequest *pmdtr = (*m_pdrgptr)[ul];
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
										CDXLTokens::PstrToken(EdxltokenMDTypeRequest));				
		
		CWStringDynamic *pstr = Pstr(pmdtr->m_sysid);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSysid), pstr);
		GPOS_DELETE(pstr);
		
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeInfo), pmdtr->m_eti);
		
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
										CDXLTokens::PstrToken(EdxltokenMDTypeRequest));				
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenMDRequest));
}



// EOF

