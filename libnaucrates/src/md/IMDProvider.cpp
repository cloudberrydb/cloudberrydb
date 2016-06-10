//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDProvider.cpp
//
//	@doc:
//		Abstract class for retrieving metadata from an external location
//---------------------------------------------------------------------------

#include "naucrates/md/IMDProvider.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		IMDProvider::PmdidTypeGPDB
//
//	@doc:
//		Return the mdid for the requested type
//
//---------------------------------------------------------------------------
IMDId *
IMDProvider::PmdidTypeGPDB
	(
	IMemoryPool *pmp,
	CSystemId
#ifdef GPOS_DEBUG
	sysid
#endif // GPOS_DEBUG
	,
	IMDType::ETypeInfo eti
	)
{
	GPOS_ASSERT(IMDId::EmdidGPDB == sysid.Emdidt());
	GPOS_ASSERT(IMDType::EtiGeneric > eti);

	switch(eti)
	{
		case IMDType::EtiInt2:
			return GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2);

		case IMDType::EtiInt4:
			return GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4);

		case IMDType::EtiInt8:
			return GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8);

		case IMDType::EtiBool:
			return GPOS_NEW(pmp) CMDIdGPDB(GPDB_BOOL);

		case IMDType::EtiOid:
			return GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID);

		default:
			return NULL;
	}
}

// EOF
