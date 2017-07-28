//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDIdGPDB.cpp
//
//	@doc:
//		Implementation of metadata identifiers
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpmd;


// initialize static members
// invalid key
CMDIdGPDB CMDIdGPDB::m_mdidInvalidKey(0, 0, 0);

// int2 mdid
CMDIdGPDB CMDIdGPDB::m_mdidInt2(GPDB_INT2);

// int4 mdid
CMDIdGPDB CMDIdGPDB::m_mdidInt4(GPDB_INT4);

// int8 mdid
CMDIdGPDB CMDIdGPDB::m_mdidInt8(GPDB_INT8);

// bool mdid
CMDIdGPDB CMDIdGPDB::m_mdidBool(GPDB_BOOL);

// oid mdid
CMDIdGPDB CMDIdGPDB::m_mdidOid(GPDB_OID);

// numeric mdid
CMDIdGPDB CMDIdGPDB::m_mdidNumeric(GPDB_NUMERIC);

// date mdid
CMDIdGPDB CMDIdGPDB::m_mdidDate(GPDB_DATE);

// time mdid
CMDIdGPDB CMDIdGPDB::m_mdidTime(GPDB_TIME);

// time with time zone mdid
CMDIdGPDB CMDIdGPDB::m_mdidTimeTz(GPDB_TIMETZ);

// timestamp mdid
CMDIdGPDB CMDIdGPDB::m_mdidTimestamp(GPDB_TIMESTAMP);

// timestamp with time zone mdid
CMDIdGPDB CMDIdGPDB::m_mdidTimestampTz(GPDB_TIMESTAMPTZ);

// absolute time mdid
CMDIdGPDB CMDIdGPDB::m_mdidAbsTime(GPDB_ABSTIME);

// relative time mdid
CMDIdGPDB CMDIdGPDB::m_mdidRelTime(GPDB_RELTIME);

// interval mdid
CMDIdGPDB CMDIdGPDB::m_mdidInterval(GPDB_INTERVAL);

// time interval mdid
CMDIdGPDB CMDIdGPDB::m_mdidTimeInterval(GPDB_TIMEINTERVAL);

// bpchar mdid
CMDIdGPDB CMDIdGPDB::m_mdidBPChar(GPDB_CHAR);

// varchar mdid
CMDIdGPDB CMDIdGPDB::m_mdidVarChar(GPDB_VARCHAR);

// text mdid
CMDIdGPDB CMDIdGPDB::m_mdidText(GPDB_TEXT);

// float4 mdid
CMDIdGPDB CMDIdGPDB::m_mdidFloat4(GPDB_FLOAT4);

// float8 mdid
CMDIdGPDB CMDIdGPDB::m_mdidFloat8(GPDB_FLOAT8);

// cash mdid
CMDIdGPDB CMDIdGPDB::m_mdidCash(GPDB_CASH);

// inet mdid
CMDIdGPDB CMDIdGPDB::m_mdidInet(GPDB_INET);

// cidr mdid
CMDIdGPDB CMDIdGPDB::m_mdidCidr(GPDB_CIDR);

// macaddr mdid
CMDIdGPDB CMDIdGPDB::m_mdidMacaddr(GPDB_MACADDR);

// count(*) mdid
CMDIdGPDB CMDIdGPDB::m_mdidCountStar(GPDB_COUNT_STAR);

// count(Any) mdid
CMDIdGPDB CMDIdGPDB::m_mdidCountAny(GPDB_COUNT_ANY);

// unknown mdid
CMDIdGPDB CMDIdGPDB::m_mdidUnknown(GPDB_UNKNOWN);

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Constructs a metadata identifier with specified oid and default version
//		of 1.0
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB
	(
	CSystemId sysid,
	OID oid
	)
	:
	m_sysid(sysid),
	m_oid(oid),
	m_ulVersionMajor(1),
	m_ulVersionMinor(0),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	if (CMDIdGPDB::m_mdidInvalidKey.OidObjectId() == oid)
	{
		// construct an invalid mdid 0.0.0
		m_ulVersionMajor = 0;
	}
		
	// serialize mdid into static string 
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Constructs a metadata identifier with specified oid and default version
//		of 1.0
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB
	(
	OID oid
	)
	:
	m_sysid(IMDId::EmdidGPDB, GPMD_GPDB_SYSID),
	m_oid(oid),
	m_ulVersionMajor(1),
	m_ulVersionMinor(0),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	if (CMDIdGPDB::m_mdidInvalidKey.OidObjectId() == oid)
	{
		// construct an invalid mdid 0.0.0
		m_ulVersionMajor = 0;
	}
	
	// TODO:  - Jan 31, 2012; supply system id in constructor
	
	// serialize mdid into static string 
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Constructs a metadata identifier
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB
	(
	OID oid,
	ULONG ulVersionMajor,
	ULONG ulVersionMinor
	)
	:
	m_sysid(IMDId::EmdidGPDB, GPMD_GPDB_SYSID),
	m_oid(oid),
	m_ulVersionMajor(ulVersionMajor),
	m_ulVersionMinor(ulVersionMinor),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	// TODO:  - Jan 31, 2012; supply system id in constructor
	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::CMDIdGPDB
//
//	@doc:
//		Copy constructor
//
//---------------------------------------------------------------------------
CMDIdGPDB::CMDIdGPDB
	(
	const CMDIdGPDB &mdidSource
	)
	:
	IMDId(),
	m_sysid(mdidSource.Sysid()),
	m_oid(mdidSource.OidObjectId()),
	m_ulVersionMajor(mdidSource.UlVersionMajor()),
	m_ulVersionMinor(mdidSource.UlVersionMinor()),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	GPOS_ASSERT(mdidSource.FValid());
	GPOS_ASSERT(IMDId::EmdidGPDB == mdidSource.Emdidt());

	// serialize mdid into static string
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdGPDB::Serialize()
{
	m_str.Reset();
	// serialize mdid as SystemType.Oid.Major.Minor
	m_str.AppendFormat(GPOS_WSZ_LIT("%d.%d.%d.%d"), Emdidt(), m_oid, m_ulVersionMajor, m_ulVersionMinor);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::Wsz
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdGPDB::Wsz() const
{
	return m_str.Wsz();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::OidObjectId
//
//	@doc:
//		Returns the object id
//
//---------------------------------------------------------------------------
OID
CMDIdGPDB::OidObjectId() const
{
	return m_oid;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::UlVersionMajor
//
//	@doc:
//		Returns the object's major version
//
//---------------------------------------------------------------------------
ULONG
CMDIdGPDB::UlVersionMajor() const
{
	return m_ulVersionMajor;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::UlVersionMinor
//
//	@doc:
//		Returns the object's minor version
//
//---------------------------------------------------------------------------
ULONG
CMDIdGPDB::UlVersionMinor() const
{
	return m_ulVersionMinor;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::FEquals
//
//	@doc:
//		Checks if the version of the current object is compatible with another version
//		of the same object
//
//---------------------------------------------------------------------------
BOOL
CMDIdGPDB::FEquals
	(
	const IMDId *pmdid
	) 
	const
{
	if (NULL == pmdid || EmdidGPDB != pmdid->Emdidt())
	{
		return false;
	}
	
	const CMDIdGPDB *pmdidGPDB = CMDIdGPDB::PmdidConvert(const_cast<IMDId *>(pmdid));
	
	return (m_oid == pmdidGPDB->OidObjectId() && m_ulVersionMajor == pmdidGPDB->UlVersionMajor() &&
			m_ulVersionMinor == pmdidGPDB->UlVersionMinor()); 
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::FValid
//
//	@doc:
//		Is the mdid valid
//
//---------------------------------------------------------------------------
BOOL
CMDIdGPDB::FValid() const
{
	return !FEquals(&CMDIdGPDB::m_mdidInvalidKey);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::Serialize
//
//	@doc:
//		Serializes the mdid as the value of the given attribute
//
//---------------------------------------------------------------------------
void
CMDIdGPDB::Serialize
	(
	CXMLSerializer * pxmlser,
	const CWStringConst *pstrAttribute
	)
	const
{
	pxmlser->AddAttribute(pstrAttribute, &m_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdGPDB::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdGPDB::OsPrint
	(
	IOstream &os
	) 
	const
{
	os << "(" << OidObjectId() << "," << 
				UlVersionMajor() << "." << UlVersionMinor() << ")";
	return os;
}

// EOF
