//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDIdCast.cpp
//
//	@doc:
//		Implementation of mdids for cast functions
//---------------------------------------------------------------------------


#include "naucrates/md/CMDIdCast.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::CMDIdCast
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdCast::CMDIdCast
	(
	CMDIdGPDB *pmdidSrc,
	CMDIdGPDB *pmdidDest
	)
	:
	m_pmdidSrc(pmdidSrc),
	m_pmdidDest(pmdidDest),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	GPOS_ASSERT(pmdidSrc->FValid());
	GPOS_ASSERT(pmdidDest->FValid());
	
	// serialize mdid into static string 
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::~CMDIdCast
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdCast::~CMDIdCast()
{
	m_pmdidSrc->Release();
	m_pmdidDest->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdCast::Serialize()
{
	// serialize mdid as SystemType.mdidSrc.mdidDest
	m_str.AppendFormat
			(
			GPOS_WSZ_LIT("%d.%d.%d.%d;%d.%d.%d"), 
			Emdidt(), 
			m_pmdidSrc->OidObjectId(),
			m_pmdidSrc->UlVersionMajor(),
			m_pmdidSrc->UlVersionMinor(),
			m_pmdidDest->OidObjectId(),
			m_pmdidDest->UlVersionMajor(),
			m_pmdidDest->UlVersionMinor()			
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::Wsz
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdCast::Wsz() const
{
	return m_str.Wsz();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::PmdidSrc
//
//	@doc:
//		Returns the source type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdCast::PmdidSrc() const
{
	return m_pmdidSrc;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::PmdidDest
//
//	@doc:
//		Returns the destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdCast::PmdidDest() const
{
	return m_pmdidDest;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::FEquals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdCast::FEquals
	(
	const IMDId *pmdid
	) 
	const
{
	if (NULL == pmdid || EmdidCastFunc != pmdid->Emdidt())
	{
		return false;
	}
	
	const CMDIdCast *pmdidCastFunc = CMDIdCast::PmdidConvert(pmdid);
	
	return m_pmdidSrc->FEquals(pmdidCastFunc->PmdidSrc()) && 
			m_pmdidDest->FEquals(pmdidCastFunc->PmdidDest()); 
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdCast::Serialize
//
//	@doc:
//		Serializes the mdid as the value of the given attribute
//
//---------------------------------------------------------------------------
void
CMDIdCast::Serialize
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
//		CMDIdCast::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdCast::OsPrint
	(
	IOstream &os
	) 
	const
{
	os << "(" << m_str.Wsz() << ")";
	return os;
}

// EOF
