//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumInt4GPDB.cpp
//
//	@doc:
//		Implementation of GPDB int4
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/base/CDatumInt4GPDB.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeInt4.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpnaucrates;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::CDatumInt4GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumInt4GPDB::CDatumInt4GPDB
	(
	CSystemId sysid,
	INT iVal,
	BOOL fNull
	)
	:
	m_pmdid(NULL),
	m_iVal(iVal),
	m_fNull(fNull)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *pmdid = dynamic_cast<const CMDTypeInt4GPDB *>(pmda->PtMDType<IMDTypeInt4>(sysid))->Pmdid();
	pmdid->AddRef();
	
	m_pmdid = pmdid;

	if (FNull())
	{
		// needed for hash computation
		m_iVal = gpos::int_max;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::CDatumInt4GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumInt4GPDB::CDatumInt4GPDB
	(
	IMDId *pmdid,
	INT iVal,
	BOOL fNull
	)
	:
	m_pmdid(pmdid),
	m_iVal(iVal),
	m_fNull(fNull)
{
	GPOS_ASSERT(NULL != m_pmdid);
	GPOS_ASSERT(GPDB_INT4_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());

	if (FNull())
	{
		// needed for hash computation
		m_iVal = gpos::int_max;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::~CDatumInt4GPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDatumInt4GPDB::~CDatumInt4GPDB()
{
	m_pmdid->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::IValue
//
//	@doc:
//		Accessor of integer value
//
//---------------------------------------------------------------------------
INT
CDatumInt4GPDB::IValue() const
{
	return m_iVal;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::FNull
//
//	@doc:
//		Accessor of is null
//
//---------------------------------------------------------------------------
BOOL
CDatumInt4GPDB::FNull() const
{
	return m_fNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::UlSize
//
//	@doc:
//		Accessor of size
//
//---------------------------------------------------------------------------
ULONG
CDatumInt4GPDB::UlSize() const
{
	return 4;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::Pmdid
//
//	@doc:
//		Accessor of type information
//
//---------------------------------------------------------------------------
IMDId *
CDatumInt4GPDB::Pmdid() const
{
	return m_pmdid;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDatumInt4GPDB::UlHash() const
{
	return gpos::UlCombineHashes(m_pmdid->UlHash(), gpos::UlHash<INT>(&m_iVal));
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::Pstr
//
//	@doc:
//		Return string representation
//
//---------------------------------------------------------------------------
const CWStringConst *
CDatumInt4GPDB::Pstr
	(
	IMemoryPool *pmp
	)
	const
{
	CWStringDynamic str(pmp);
	if (!FNull())
	{
		str.AppendFormat(GPOS_WSZ_LIT("%d"), m_iVal);
	}
	else
	{
		str.AppendFormat(GPOS_WSZ_LIT("null"));
	}

	return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::FMatch
//
//	@doc:
//		Matches the values of datums
//
//---------------------------------------------------------------------------
BOOL
CDatumInt4GPDB::FMatch
	(
	const IDatum *pdatum
	)
	const
{
	if(!pdatum->Pmdid()->FEquals(m_pmdid))
	{
		return false;
	}

	const CDatumInt4GPDB *pdatumint4 = dynamic_cast<const CDatumInt4GPDB *>(pdatum);

	if(!pdatumint4->FNull() && !FNull())
	{
		return (pdatumint4->IValue() == IValue());
	}

	if(pdatumint4->FNull() && FNull())
	{
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::PdatumCopy
//
//	@doc:
//		Returns a copy of the datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumInt4GPDB::PdatumCopy
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();
	return GPOS_NEW(pmp) CDatumInt4GPDB(m_pmdid, m_iVal, m_fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumInt4GPDB::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDatumInt4GPDB::OsPrint
	(
	IOstream &os
	)
	const
{
	if (!FNull())
	{
		os << m_iVal;
	}
	else
	{
		os << "null";
	}

	return os;
}

// EOF

