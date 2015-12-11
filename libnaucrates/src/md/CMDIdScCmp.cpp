//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDIdScCmp.cpp
//
//	@doc:
//		Implementation of mdids for scalar comparisons functions
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIdScCmp.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::CMDIdScCmp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDIdScCmp::CMDIdScCmp
	(
	CMDIdGPDB *pmdidLeft,
	CMDIdGPDB *pmdidRight,
	IMDType::ECmpType ecmpt
	)
	:
	m_pmdidLeft(pmdidLeft),
	m_pmdidRight(pmdidRight),
	m_ecmpt(ecmpt),
	m_str(m_wszBuffer, GPOS_ARRAY_SIZE(m_wszBuffer))
{
	GPOS_ASSERT(pmdidLeft->FValid());
	GPOS_ASSERT(pmdidRight->FValid());
	GPOS_ASSERT(IMDType::EcmptOther != ecmpt);
	
	GPOS_ASSERT(pmdidLeft->Sysid().FEquals(pmdidRight->Sysid()));
	
	// serialize mdid into static string 
	Serialize();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::~CMDIdScCmp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDIdScCmp::~CMDIdScCmp()
{
	m_pmdidLeft->Release();
	m_pmdidRight->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::Serialize
//
//	@doc:
//		Serialize mdid into static string
//
//---------------------------------------------------------------------------
void
CMDIdScCmp::Serialize()
{
	// serialize mdid as SystemType.mdidLeft;mdidRight;CmpType
	m_str.AppendFormat
			(
			GPOS_WSZ_LIT("%d.%d.%d.%d;%d.%d.%d;%d"), 
			Emdidt(), 
			m_pmdidLeft->OidObjectId(),
			m_pmdidLeft->UlVersionMajor(),
			m_pmdidLeft->UlVersionMinor(),
			m_pmdidRight->OidObjectId(),
			m_pmdidRight->UlVersionMajor(),
			m_pmdidRight->UlVersionMinor(),
			m_ecmpt
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::Wsz
//
//	@doc:
//		Returns the string representation of the mdid
//
//---------------------------------------------------------------------------
const WCHAR *
CMDIdScCmp::Wsz() const
{
	return m_str.Wsz();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::PmdidLeft
//
//	@doc:
//		Returns the source type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdScCmp::PmdidLeft() const
{
	return m_pmdidLeft;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::PmdidRight
//
//	@doc:
//		Returns the destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDIdScCmp::PmdidRight() const
{
	return m_pmdidRight;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::UlHash
//
//	@doc:
//		Computes the hash value for the metadata id
//
//---------------------------------------------------------------------------
ULONG
CMDIdScCmp::UlHash() const
{
	return gpos::UlCombineHashes
								(
								Emdidt(), 
								gpos::UlCombineHashes(m_pmdidLeft->UlHash(), m_pmdidRight->UlHash())
								);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::FEquals
//
//	@doc:
//		Checks if the mdids are equal
//
//---------------------------------------------------------------------------
BOOL
CMDIdScCmp::FEquals
	(
	const IMDId *pmdid
	) 
	const
{
	if (NULL == pmdid || EmdidScCmp != pmdid->Emdidt())
	{
		return false;
	}
	
	const CMDIdScCmp *pmdidScCmp = CMDIdScCmp::PmdidConvert(pmdid);
	
	return m_pmdidLeft->FEquals(pmdidScCmp->PmdidLeft()) && 
			m_pmdidRight->FEquals(pmdidScCmp->PmdidRight()) &&
			m_ecmpt == pmdidScCmp->Ecmpt(); 
}

//---------------------------------------------------------------------------
//	@function:
//		CMDIdScCmp::Serialize
//
//	@doc:
//		Serializes the mdid as the value of the given attribute
//
//---------------------------------------------------------------------------
void
CMDIdScCmp::Serialize
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
//		CMDIdScCmp::OsPrint
//
//	@doc:
//		Debug print of the id in the provided stream
//
//---------------------------------------------------------------------------
IOstream &
CMDIdScCmp::OsPrint
	(
	IOstream &os
	) 
	const
{
	os << "(" << m_str.Wsz() << ")";
	return os;
}

// EOF
