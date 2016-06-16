//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDProviderGeneric.cpp
//
//	@doc:
//		Implementation of a generic MD provider.
//---------------------------------------------------------------------------

#include "gpos/io/COstreamString.h"
#include "gpos/memory/IMemoryPool.h"

#include "naucrates/md/CMDProviderGeneric.h"
#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CMDTypeBoolGPDB.h"
#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/md/CDXLColStats.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "gpos/error/CAutoTrace.h"
#include "naucrates/exception.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::CMDProviderGeneric
//
//	@doc:
//		Constructs a file-based metadata provider
//
//---------------------------------------------------------------------------
CMDProviderGeneric::CMDProviderGeneric
	(
	IMemoryPool *pmp
	)
{
	// TODO:  - Jan 25, 2012; those should not be tied to a particular system
	m_pmdidInt2 = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2);
	m_pmdidInt4 = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4);
	m_pmdidInt8 = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8);
	m_pmdidBool = GPOS_NEW(pmp) CMDIdGPDB(GPDB_BOOL);
	m_pmdidOid = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::~CMDProviderGeneric
//
//	@doc:
//		Destructor 
//
//---------------------------------------------------------------------------
CMDProviderGeneric::~CMDProviderGeneric()
{
	m_pmdidInt2->Release();
	m_pmdidInt4->Release();
	m_pmdidInt8->Release();
	m_pmdidBool->Release();
	m_pmdidOid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::Pmdid
//
//	@doc:
//		return the mdid of a requested type
//
//---------------------------------------------------------------------------
IMDId *
CMDProviderGeneric::Pmdid
	(
	IMDType::ETypeInfo eti
	) 
	const
{
	GPOS_ASSERT(IMDType::EtiGeneric > eti);
	
	switch(eti)
	{
		case IMDType::EtiInt2:
			return m_pmdidInt2;

		case IMDType::EtiInt4:
			return m_pmdidInt4;

		case IMDType::EtiInt8:
			return m_pmdidInt8;

		case IMDType::EtiBool:
			return m_pmdidBool;

		case IMDType::EtiOid:
			return m_pmdidOid;

		default:
			return NULL;
	}
}	

//---------------------------------------------------------------------------
//	@function:
//		CMDProviderGeneric::SysidDefault
//
//	@doc:
//		Get the default system id of the MD provider
//
//---------------------------------------------------------------------------
CSystemId 
CMDProviderGeneric::SysidDefault() const
{
	return CSystemId(IMDId::EmdidGPDB, GPMD_GPDB_SYSID);
}

// EOF
