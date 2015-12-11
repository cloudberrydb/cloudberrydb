//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDProviderMemory.cpp
//
//	@doc:
//		Implementation of a memory-based metadata provider, which loads all
//		objects in memory and provides a function for looking them up by id.
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
#include "gpos/task/CWorker.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"
#include "gpos/error/CAutoTrace.h"

#include "naucrates/md/CMDProviderMemory.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/exception.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::CMDProviderMemory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDProviderMemory::CMDProviderMemory
	(
	IMemoryPool *pmp,
	const CHAR *szFileName
	)
	:
	m_pmdmap(NULL)
{
	GPOS_ASSERT(NULL != szFileName);
	
	// read DXL file
	CAutoRg<CHAR> a_szDXL;
	a_szDXL = CDXLUtils::SzRead(pmp, szFileName);

	CAutoRef<DrgPimdobj> a_pdrgpmdobj;
	a_pdrgpmdobj = CDXLUtils::PdrgpmdobjParseDXL(pmp, a_szDXL.Rgt(), NULL /*szXSDPath*/);
	
#ifdef GPOS_DEBUG
	CWorker::PwrkrSelf()->ResetTimeSlice();
#endif // GPOS_DEBUG

	
	LoadMetadataObjectsFromArray(pmp, a_pdrgpmdobj.Pt());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::CMDProviderMemory
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDProviderMemory::CMDProviderMemory
	(
	IMemoryPool *pmp,
	DrgPimdobj *pdrgpmdobj
	)
	:
	m_pmdmap(NULL)
{
	LoadMetadataObjectsFromArray(pmp, pdrgpmdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::LoadMetadataObjectsFromArray
//
//	@doc:
//		Loads the metadata objects from the given file
//
//---------------------------------------------------------------------------
void
CMDProviderMemory::LoadMetadataObjectsFromArray
	(
	IMemoryPool *pmp,
	DrgPimdobj *pdrgpmdobj
	)
{
	GPOS_ASSERT(NULL != pdrgpmdobj);

	// load metadata objects from the file
	CAutoRef<MDMap> a_pmdmap;
	m_pmdmap = GPOS_NEW(pmp) MDMap(pmp);
	a_pmdmap = m_pmdmap;

	const ULONG ulSize = pdrgpmdobj->UlLength();

	// load objects into the hash map
	for (ULONG ul = 0; ul < ulSize; ul++)
	{
		GPOS_CHECK_ABORT;

		IMDCacheObject *pmdobj = (*pdrgpmdobj)[ul];
		IMDId *pmdidKey = pmdobj->Pmdid();
		pmdidKey->AddRef();
		CAutoRef<IMDId> a_pmdidKey;
		a_pmdidKey = pmdidKey;
		
		CAutoP<CWStringDynamic> a_pstr;
		a_pstr = CDXLUtils::PstrSerializeMDObj(pmp, pmdobj, true /*fSerializeHeaders*/, false /*findent*/);
		
		GPOS_CHECK_ABORT;
		BOOL fInserted = m_pmdmap->FInsert(pmdidKey, a_pstr.Pt());
		if (!fInserted)
		{
			
			GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryDuplicate, pmdidKey->Wsz());
		}
		(void) a_pmdidKey.PtReset();
		(void) a_pstr.PtReset();
	}
	
	// safely completed loading
	(void) a_pmdmap.PtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::~CMDProviderMemory
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDProviderMemory::~CMDProviderMemory()
{
	CRefCount::SafeRelease(m_pmdmap);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::PstrObject
//
//	@doc:
//		Returns the DXL of the requested object in the provided memory pool
//
//---------------------------------------------------------------------------
CWStringBase *
CMDProviderMemory::PstrObject
	(
	IMemoryPool *pmp,
	CMDAccessor *, //pmda
	IMDId *pmdid
	) 
	const
{
	GPOS_ASSERT(NULL != m_pmdmap);

	const CWStringDynamic *pstrObj = m_pmdmap->PtLookup(pmdid);
	
	// result string
	CAutoP<CWStringDynamic> a_pstrResult;

	a_pstrResult = NULL;
	
	if (NULL == pstrObj)
	{
		// Relstats and colstats are special as they may not
		// exist in the metadata file. Provider must return dummy objects
		// in this case.
		switch(pmdid->Emdidt())
		{
			case IMDId::EmdidRelStats:
			{
				pmdid->AddRef();
				CAutoRef<CDXLRelStats> a_pdxlrelstats;
				a_pdxlrelstats = CDXLRelStats::PdxlrelstatsDummy(pmp, pmdid);
				a_pstrResult = CDXLUtils::PstrSerializeMDObj(pmp, a_pdxlrelstats.Pt(), true /*fSerializeHeaders*/, false /*findent*/);
				break;
			}
			case IMDId::EmdidColStats:
			{
				CAutoP<CWStringDynamic> a_pstr;
				a_pstr = GPOS_NEW(pmp) CWStringDynamic(pmp, pmdid->Wsz());
				CAutoP<CMDName> a_pmdname;
				a_pmdname = GPOS_NEW(pmp) CMDName(pmp, a_pstr.Pt());
				pmdid->AddRef();
				CAutoRef<CDXLColStats> a_pdxlcolstats;
				a_pdxlcolstats = CDXLColStats::PdxlcolstatsDummy(pmp, pmdid, a_pmdname.Pt(), CStatistics::DDefaultColumnWidth /* dWidth */);
				a_pmdname.PtReset();
				a_pstrResult = CDXLUtils::PstrSerializeMDObj(pmp, a_pdxlcolstats.Pt(), true /*fSerializeHeaders*/, false /*findent*/);
				break;
			}
			default:
			{
				GPOS_RAISE(gpdxl::ExmaMD, gpdxl::ExmiMDCacheEntryNotFound, pmdid->Wsz());
			}
		}
	}
	else
	{
		// copy string into result
		a_pstrResult = GPOS_NEW(pmp) CWStringDynamic(pmp, pstrObj->Wsz());
	}
	
	GPOS_ASSERT(NULL != a_pstrResult.Pt());
	
	return a_pstrResult.PtReset();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderMemory::Pmdid
//
//	@doc:
//		Returns the mdid for the requested system and type info. 
//		The caller takes ownership over the object.
//
//---------------------------------------------------------------------------
IMDId *
CMDProviderMemory::Pmdid
	(
	IMemoryPool *pmp,
	CSystemId sysid,
	IMDType::ETypeInfo eti
	) 
	const
{
	return PmdidTypeGPDB(pmp, sysid, eti);
}

// EOF
