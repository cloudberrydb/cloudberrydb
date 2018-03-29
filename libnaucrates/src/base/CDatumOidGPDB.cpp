//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDatumOidGPDB.cpp
//
//	@doc:
//		Implementation of GPDB oid datum
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/base/CDatumOidGPDB.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeOid.h"
#include "naucrates/md/CMDTypeOidGPDB.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpnaucrates;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::CDatumOidGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumOidGPDB::CDatumOidGPDB
	(
	CSystemId sysid,
	OID oidVal,
	BOOL fNull
	)
	:
	m_pmdid(NULL),
	m_oidVal(oidVal),
	m_fNull(fNull)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *pmdid = dynamic_cast<const CMDTypeOidGPDB *>(pmda->PtMDType<IMDTypeOid>(sysid))->Pmdid();
	pmdid->AddRef();

	m_pmdid = pmdid;

	if (FNull())
	{
		// needed for hash computation
		m_oidVal = gpos::int_max;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::CDatumOidGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumOidGPDB::CDatumOidGPDB
	(
	IMDId *pmdid,
	OID oidVal,
	BOOL fNull
	)
	:
	m_pmdid(pmdid),
	m_oidVal(oidVal),
	m_fNull(fNull)
{
	GPOS_ASSERT(NULL != m_pmdid);
	GPOS_ASSERT(GPDB_OID_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());

	if (FNull())
	{
		// needed for hash computation
		m_oidVal = gpos::int_max;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::~CDatumOidGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDatumOidGPDB::~CDatumOidGPDB()
{
	m_pmdid->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::OidValue
//
//	@doc:
//		Accessor of oid value
//
//---------------------------------------------------------------------------
OID
CDatumOidGPDB::OidValue() const
{
	return m_oidVal;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::FNull
//
//	@doc:
//		Accessor of is null
//
//---------------------------------------------------------------------------
BOOL
CDatumOidGPDB::FNull() const
{
	return m_fNull;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::UlSize
//
//	@doc:
//		Accessor of size
//
//---------------------------------------------------------------------------
ULONG
CDatumOidGPDB::UlSize() const
{
	return 4;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::Pmdid
//
//	@doc:
//		Accessor of type information
//
//---------------------------------------------------------------------------
IMDId *
CDatumOidGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDatumOidGPDB::UlHash() const
{
	return gpos::UlCombineHashes(m_pmdid->UlHash(), gpos::UlHash<OID>(&m_oidVal));
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::Pstr
//
//	@doc:
//		Return string representation
//
//---------------------------------------------------------------------------
const CWStringConst *
CDatumOidGPDB::Pstr
	(
	IMemoryPool *pmp
	)
	const
{
	CWStringDynamic str(pmp);
	if (!FNull())
	{
		str.AppendFormat(GPOS_WSZ_LIT("%d"), m_oidVal);
	}
	else
	{
		str.AppendFormat(GPOS_WSZ_LIT("null"));
	}

	return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::FMatch
//
//	@doc:
//		Matches the values of datums
//
//---------------------------------------------------------------------------
BOOL
CDatumOidGPDB::FMatch
	(
	const IDatum *pdatum
	)
	const
{
	if(!pdatum->Pmdid()->FEquals(m_pmdid))
	{
		return false;
	}

	const CDatumOidGPDB *pdatumoid = dynamic_cast<const CDatumOidGPDB *>(pdatum);

	if(!pdatumoid->FNull() && !FNull())
	{
		return (pdatumoid->OidValue() == OidValue());
	}

	if(pdatumoid->FNull() && FNull())
	{
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::PdatumCopy
//
//	@doc:
//		Returns a copy of the datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumOidGPDB::PdatumCopy
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();
	return GPOS_NEW(pmp) CDatumOidGPDB(m_pmdid, m_oidVal, m_fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumOidGPDB::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDatumOidGPDB::OsPrint
	(
	IOstream &os
	)
	const
{
	if (!FNull())
	{
		os << m_oidVal;
	}
	else
	{
		os << "null";
	}

	return os;
}

// EOF
