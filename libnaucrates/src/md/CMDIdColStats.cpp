//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDIdColStats.cpp
//
//	@doc:
//		Implementation of mdids for column statistics
//---------------------------------------------------------------------------


#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::CMDIdColStats
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdColStats::CMDIdColStats
	(
	CMDIdGPDB *pmdidRel,
	ULONG ulPos
	)
	:
	m_pmdidRel(pmdidRel),
	m_ulPos(ulPos),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	GPOS_ASSERT(pmdidRel->FValid());
	
	// serialize mdid into static string 
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::~CMDIdColStats
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdColStats::~CMDIdColStats()
{
	m_pmdidRel->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdColStats::Serialize()
{
	// serialize mdid as SystemType.Oid.Major.Minor.Attno
	m_str.AppendFormat
			(
			GPOS_WSZ_LIT("%d.%d.%d.%d.%d"), 
			Emdidt(), 
			m_pmdidRel->OidObjectId(),
			m_pmdidRel->UlVersionMajor(),
			m_pmdidRel->UlVersionMinor(),
			m_ulPos
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::Wsz
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdColStats::Wsz() const
{
	return m_str.Wsz();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::PmdidRel
//
//	@doc:
//		Returns the base relation id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdColStats::PmdidRel() const
{
	return m_pmdidRel;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::UlPos
//
//	@doc:
//		Returns the attribute number
//
//---------------------------------------------------------------------------
ULONG
CMDIdColStats::UlPos() const
{
	return m_ulPos;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::FEquals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdColStats::FEquals
	(
	const IMDId *pmdid
	) 
	const
{
	if (NULL == pmdid || EmdidColStats != pmdid->Emdidt())
	{
		return false;
	}
	
	const CMDIdColStats *pmdidColStats = CMDIdColStats::PmdidConvert(pmdid);
	
	return m_pmdidRel->FEquals(pmdidColStats->PmdidRel()) && 
			m_ulPos == pmdidColStats->UlPos(); 
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdColStats::Serialize
//
//	@doc:
//		Serializes the mdid as the value of the given attribute
//
//---------------------------------------------------------------------------
void
CMDIdColStats::Serialize
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
//		CMDIdColStats::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdColStats::OsPrint
	(
	IOstream &os
	) 
	const
{
	os << "(" << m_str.Wsz() << ")";
	return os;
}

// EOF
