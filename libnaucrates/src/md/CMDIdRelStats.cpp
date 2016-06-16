//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdRelStats.cpp
//
//	@doc:
//		Implementation of mdids for relation statistics
//---------------------------------------------------------------------------


#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::CMDIdRelStats
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdRelStats::CMDIdRelStats
	(
	CMDIdGPDB *pmdidRel
	)
	:
	m_pmdidRel(pmdidRel),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	// serialize mdid into static string 
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::~CMDIdRelStats
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdRelStats::~CMDIdRelStats()
{
	m_pmdidRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdRelStats::Serialize()
{
	// serialize mdid as SystemType.Oid.Major.Minor
	m_str.AppendFormat
			(
			GPOS_WSZ_LIT("%d.%d.%d.%d"), 
			Emdidt(), 
			m_pmdidRel->OidObjectId(),
			m_pmdidRel->UlVersionMajor(),
			m_pmdidRel->UlVersionMinor()
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::Wsz
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdRelStats::Wsz() const
{
	return m_str.Wsz();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::PmdidRel
//
//	@doc:
//		Returns the base relation id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdRelStats::PmdidRel() const
{
	return m_pmdidRel;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::FEquals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdRelStats::FEquals
	(
	const IMDId *pmdid
	) 
	const
{
	if (NULL == pmdid || EmdidRelStats != pmdid->Emdidt())
	{
		return false;
	}
	
	const CMDIdRelStats *pmdidRelStats = CMDIdRelStats::PmdidConvert(pmdid);
	
	return m_pmdidRel->FEquals(pmdidRelStats->PmdidRel()); 
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdRelStats::Serialize
//
//	@doc:
//		Serializes the mdid as the value of the given attribute
//
//---------------------------------------------------------------------------
void
CMDIdRelStats::Serialize
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
//		CMDIdRelStats::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdRelStats::OsPrint
	(
	IOstream &os
	) 
	const
{
	os << "(" << m_str.Wsz() << ")";
	return os;
}

// EOF
