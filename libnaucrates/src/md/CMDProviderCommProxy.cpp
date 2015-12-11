//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderCommProxy.cpp
//
//	@doc:
//		Implementation of a MD proxy for answering communicator-based requests
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

#include "naucrates/md/CMDProviderCommProxy.h"
#include "naucrates/md/CMDRequest.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/mdcache/CAutoMDAccessor.h"
#include "gpopt/mdcache/CMDCache.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;
using namespace gpnaucrates;

// default id for the source system
const CSystemId sysidDefault(IMDId::EmdidGPDB, GPOS_WSZ_STR_LENGTH("GPDB"));

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommProxy::CMDProviderCommProxy
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDProviderCommProxy::CMDProviderCommProxy
	(
	IMemoryPool *pmp,
	IMDProvider *pmdp
	)
	:
	m_pmp(pmp),
	m_pmdp(pmdp)
{
	GPOS_ASSERT(NULL != m_pmp);
	GPOS_ASSERT(NULL != m_pmdp);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommProxy::~CMDProviderCommProxy
//
//	@doc:
//		Dtor 
//
//---------------------------------------------------------------------------
CMDProviderCommProxy::~CMDProviderCommProxy()
{}


//---------------------------------------------------------------------------
//	@function:
//		CMDProviderCommProxy::PstrObject
//
//	@doc:
//		Parse the DXL MD request and return the requested objects again in 
//		DXL format
//
//---------------------------------------------------------------------------
CWStringBase *
CMDProviderCommProxy::PstrObject
	(
	const WCHAR *wszMDRequest
	) 
	const
{
	GPOS_ASSERT(NULL != wszMDRequest);
	
	// extract request components
	CMDRequest *pmdr = CDXLUtils::PmdrequestParseDXL(m_pmp, wszMDRequest, NULL /* szXSDPath */);
	DrgPmdid *pdrgpmdid = pmdr->Pdrgpmdid();
	CMDRequest::DrgPtr *pdrgptr = pmdr->Pdrgptr();
	
	GPOS_ASSERT(NULL != pdrgpmdid);
	GPOS_ASSERT(NULL != pdrgptr);
	
	CWStringDynamic *pstrMedatada = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);
	
	// retrieve MD objects
	const ULONG ulMdids = pdrgpmdid->UlLength();
	
	{
		m_pmdp->AddRef();
		CAutoMDAccessor amda(m_pmp, m_pmdp, sysidDefault, CMDCache::Pcache());;

		for (ULONG ul = 0; ul < ulMdids; ul++)
		{
			IMDId *pmdid = (*pdrgpmdid)[ul];
			CWStringBase *pstr = m_pmdp->PstrObject(m_pmp, amda.Pmda(), pmdid);
			pstrMedatada->Append(pstr);
		
			GPOS_DELETE(pstr);
			GPOS_CHECK_ABORT;
		}
	}
	
	// retrieve type info
	const ULONG ulTypes = pdrgptr->UlLength();
	
	for (ULONG ul = 0; ul < ulTypes; ul++)
	{
		CMDRequest::SMDTypeRequest *pmdtr = (*pdrgptr)[ul];
		IMDId *pmdid = m_pmdp->Pmdid(m_pmp, pmdtr->m_sysid, pmdtr->m_eti);
		
		CWStringBase *pstr = CDXLUtils::PstrSerializeMetadata(m_pmp, pmdid, true /*fDocumentHeaderFooter*/ , false /*fIndent*/);
		pstrMedatada->Append(pstr);
		
		GPOS_DELETE(pstr);
		pmdid->Release();
		GPOS_CHECK_ABORT;
	}
	
	pmdr->Release();
	
	return pstrMedatada;
}

// EOF

