//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDatumBoolGPDB.cpp
//
//	@doc:
//		Implementation of GPDB bool
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/base/CDatumBoolGPDB.h"
#include "gpopt/base/CAutoOptCtxt.h"
#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDType.h"
#include "naucrates/md/IMDTypeBool.h"
#include "naucrates/md/CMDIdGPDB.h"

using namespace gpnaucrates;
using namespace gpopt;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::CDatumBoolGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumBoolGPDB::CDatumBoolGPDB
	(
	CSystemId sysid,
	BOOL fVal,
	BOOL fNull
	)
	:
	m_fVal(fVal),
	m_fNull(fNull)
{
	CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	IMDId *pmdid = dynamic_cast<const CMDTypeBoolGPDB *>(pmda->PtMDType<IMDTypeBool>(sysid))->Pmdid();
	pmdid->AddRef();
	
	m_pmdid = pmdid;

	if (FNull())
	{
		// needed for hash computation
		m_fVal = false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::CDatumBoolGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDatumBoolGPDB::CDatumBoolGPDB
	(
	IMDId *pmdid,
	BOOL fVal,
	BOOL fNull
	)
	:
	m_pmdid(pmdid),
	m_fVal(fVal),
	m_fNull(fNull)
{
	GPOS_ASSERT(NULL != m_pmdid);
	GPOS_ASSERT(GPDB_BOOL_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());

	if (FNull())
	{
		// needed for hash computation
		m_fVal = false;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::~CDatumBoolGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDatumBoolGPDB::~CDatumBoolGPDB()
{
	m_pmdid->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::FValue
//
//	@doc:
//		Accessor of boolean value
//
//---------------------------------------------------------------------------
BOOL
CDatumBoolGPDB::FValue() const
{
	return m_fVal;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::FNull
//
//	@doc:
//		Accessor of is null
//
//---------------------------------------------------------------------------
BOOL
CDatumBoolGPDB::FNull() const
{
	return m_fNull;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::UlSize
//
//	@doc:
//		Accessor of size
//
//---------------------------------------------------------------------------
ULONG
CDatumBoolGPDB::UlSize() const
{
	return 1;
}


//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::Pmdid
//
//	@doc:
//		Accessor of type information (MDId)
//
//---------------------------------------------------------------------------
IMDId *
CDatumBoolGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::UlHash
//
//	@doc:
//		Hash function
//
//---------------------------------------------------------------------------
ULONG
CDatumBoolGPDB::UlHash() const
{
	return gpos::UlCombineHashes(m_pmdid->UlHash(), gpos::UlHash<BOOL>(&m_fVal));
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::Pstr
//
//	@doc:
//		Return string representation
//
//---------------------------------------------------------------------------
const CWStringConst *
CDatumBoolGPDB::Pstr
	(
	IMemoryPool *pmp
	)
	const
{
	CWStringDynamic str(pmp);
	if (!FNull())
	{
		str.AppendFormat(GPOS_WSZ_LIT("%d"), m_fVal);
	}
	else
	{
		str.AppendFormat(GPOS_WSZ_LIT("null"));
	}

	return GPOS_NEW(pmp) CWStringConst(pmp, str.Wsz());
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::FMatch
//
//	@doc:
//		Matches the values of datums
//
//---------------------------------------------------------------------------
BOOL
CDatumBoolGPDB::FMatch
	(
	const IDatum *pdatum
	)
	const
{
	if(!pdatum->Pmdid()->FEquals(m_pmdid))
	{
		return false;
	}

	const CDatumBoolGPDB *pdatumbool = dynamic_cast<const CDatumBoolGPDB *>(pdatum);

	if(!pdatumbool->FNull() && !FNull())
	{
		return (pdatumbool->FValue() == FValue());
	}

	if(pdatumbool->FNull() && FNull())
	{
		return true;
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::PdatumCopy
//
//	@doc:
//		Returns a copy of the datum
//
//---------------------------------------------------------------------------
IDatum *
CDatumBoolGPDB::PdatumCopy
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();
	return GPOS_NEW(pmp) CDatumBoolGPDB(m_pmdid, m_fVal, m_fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDatumBoolGPDB::OsPrint
//
//	@doc:
//		Debug print
//
//---------------------------------------------------------------------------
IOstream &
CDatumBoolGPDB::OsPrint
	(
	IOstream &os
	)
	const
{
	if (!FNull())
	{
		os << m_fVal;
	}
	else
	{
		os << "null";
	}

	return os;
}

// EOF

