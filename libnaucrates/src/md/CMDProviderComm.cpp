//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderComm.cpp
//
//	@doc:
//		Implementation of a communicator-based metadata provider
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/io/COstreamString.h"
#include "gpos/memory/IMemoryPool.h"

#include "naucrates/comm/CCommunicator.h"
#include "naucrates/comm/CCommMessage.h"

#include "naucrates/md/CMDRequest.h"
#include "naucrates/md/CMDProviderComm.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/exception.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;
using namespace gpnaucrates;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderComm::CMDProviderComm
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDProviderComm::CMDProviderComm
	(
	IMemoryPool *pmp,
	CCommunicator *pcomm
	)
	:
	m_pmp(pmp),
	m_pcomm(pcomm)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pcomm);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderComm::~CMDProviderComm
//
//	@doc:
//		Dtor 
//
//---------------------------------------------------------------------------
CMDProviderComm::~CMDProviderComm()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMDProviderComm::PstrObject
//
//	@doc:
//		Returns the DXL of the requested object in the provided memory pool
//
//---------------------------------------------------------------------------
CWStringBase *
CMDProviderComm::PstrObject
	(
	IMemoryPool *pmp,
	CMDAccessor *,//pmda,
	IMDId *pmdid
	) 
	const
{
	CWStringDynamic strMDRequest(pmp);
	COstreamString oss(&strMDRequest);

	// serialize mdid into an MD request message 
	CDXLUtils::SerializeMDRequest(pmp, pmdid, oss, true /* fDocumentHeaderFooter */, false /*fIndent */);
	
	return PstrRequestMD(pmp, &strMDRequest);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderComm::PstrRequestMD
//
//	@doc:
//		Request metadata
//
//---------------------------------------------------------------------------
CWStringBase *
CMDProviderComm::PstrRequestMD
	(
	IMemoryPool *pmp,
	const CWStringDynamic *pstrRequest
	) 
	const
{
	// send message
	CCommMessage msgRequest(CCommMessage::EcmtMDRequest, pstrRequest->Wsz(), 0 /*ullUserData*/);
	CCommMessage *pmsgResponse = m_pcomm->PmsgSendRequest(&msgRequest);

	GPOS_ASSERT(CCommMessage::EcmtMDResponse == pmsgResponse->Ecmt());
	GPOS_ASSERT(0 < GPOS_WSZ_LENGTH(pmsgResponse->Wsz()));
	
	CWStringConst *pstrResponse = GPOS_NEW(pmp) CWStringConst(pmp, pmsgResponse->Wsz());
	
	GPOS_DELETE_ARRAY(pmsgResponse->Wsz());
	GPOS_DELETE(pmsgResponse);
	
	return pstrResponse;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderComm::Pmdid
//
//	@doc:
//		Returns the mdid for the requested system and type info. 
//		The caller takes ownership over the object.
//
//---------------------------------------------------------------------------
IMDId *
CMDProviderComm::Pmdid
	(
	IMemoryPool *pmp,
	CSystemId sysid,
	IMDType::ETypeInfo eti
	) 
	const
{
	CMDRequest *pmdr = GPOS_NEW(pmp) CMDRequest(pmp, GPOS_NEW(pmp) CMDRequest::SMDTypeRequest(sysid, eti));
	
	CWStringDynamic strMDRequest(pmp);
	COstreamString oss(&strMDRequest);

	// serialize mdid into an MD request message 
	CDXLUtils::SerializeMDRequest(pmp, pmdr, oss, true /* fDocumentHeaderFooter */, false /*fIndent */);
	
	CWStringBase *pstrResponse = PstrRequestMD(pmp, &strMDRequest);
	
	IMDId *pmdid = CDXLUtils::PmdidParseDXL(pmp, pstrResponse, NULL /* xsdPath */);
	
	GPOS_DELETE(pstrResponse);
	pmdr->Release();
	
	return pmdid;
}

// EOF
