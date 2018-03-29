//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDatumInt8GPDB.cpp
//
//	@doc:
//		Implementation of GPDB Int8
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/base/CDatumInt8GPDB.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeInt8.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::CDatumInt8GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumInt8GPDB::CDatumInt8GPDB
	(
	CSystemId sysid,
	LINT lVal,
	BOOL fNull
	)
	:
	m_pmdid(NULL),
	m_lVal(lVal),
	m_fNull(fNull)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *pmdid = dynamic_cast<const CMDTypeInt8GPDB *>(pmda->PtMDType<IMDTypeInt8>(sysid))->Pmdid();
	pmdid->AddRef();
	m_pmdid = pmdid;
	
	if (FNull())
	{
		// needed for hash computation
		m_lVal = LINT(gpos::ulong_max >> 1);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::CDatumInt8GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumInt8GPDB::CDatumInt8GPDB
	(
	IMDId *pmdid,
	LINT lVal,
	BOOL fNull
	)
	:
	m_pmdid(pmdid),
	m_lVal(lVal),
	m_fNull(fNull)
{
	GPOS_ASSERT(NULL != m_pmdid);
	GPOS_ASSERT(GPDB_INT8_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());

	if (FNull())
	{
		// needed for hash computation
		m_lVal = LINT(gpos::ulong_max >> 1);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::~CDatumInt8GPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDatumInt8GPDB::~CDatumInt8GPDB()
{
	m_pmdid->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::LValue
//
//	@doc:
//		Accessor of integer value
//
//---------------------------------------------------------------------------
LINT
CDatumInt8GPDB::LValue() const
{
	return m_lVal;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::FNull
//
//	@doc:
//		Accessor of is null
//
//---------------------------------------------------------------------------
BOOL
CDatumInt8GPDB::FNull() const
{
	return m_fNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::UlSize
//
//	@doc:
//		Accessor of size
//
//---------------------------------------------------------------------------
ULONG
CDatumInt8GPDB::UlSize() const
{
	return 8;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::Pmdid
//
//	@doc:
//		Accessor of type information
//
//---------------------------------------------------------------------------
IMDId *
CDatumInt8GPDB::Pmdid() const
{
	return m_pmdid;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDatumInt8GPDB::UlHash() const
{
	return gpos::UlCombineHashes(m_pmdid->UlHash(), gpos::UlHash<LINT>(&m_lVal));
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::Pstr
//
//	@doc:
//		Return string representation
//
//---------------------------------------------------------------------------
const CWStringConst *
CDatumInt8GPDB::Pstr
	(
	IMemoryPool *pmp
	)
	const
{
	CWStringDynamic str(pmp);
	if (!FNull())
	{
		str.AppendFormat(GPOS_WSZ_LIT("%ld"), m_lVal);
	}
	else
	{
		str.AppendFormat(GPOS_WSZ_LIT("null"));
	}

	return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::FMatch
//
//	@doc:
//		Matches the values of datums
//
//---------------------------------------------------------------------------
BOOL
CDatumInt8GPDB::FMatch
	(
	const IDatum *pdatum
	)
	const
{
	if(!m_pmdid->FEquals(pdatum->Pmdid()))
	{
		return false;
	}

	const CDatumInt8GPDB *pdatumInt8 = dynamic_cast<const CDatumInt8GPDB *>(pdatum);

	if(!pdatumInt8->FNull() && !FNull())
	{
		return (pdatumInt8->LValue() == LValue());
	}

	if(pdatumInt8->FNull() && FNull())
	{
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::PdatumCopy
//
//	@doc:
//		Returns a copy of the datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumInt8GPDB::PdatumCopy
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();
	return GPOS_NEW(pmp) CDatumInt8GPDB(m_pmdid, m_lVal, m_fNull);
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt8GPDB::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDatumInt8GPDB::OsPrint
	(
	IOstream &os
	)
	const
{
	if (!FNull())
	{
		os << m_lVal;
	}
	else
	{
		os << "null";
	}

	return os;
}

// EOF

