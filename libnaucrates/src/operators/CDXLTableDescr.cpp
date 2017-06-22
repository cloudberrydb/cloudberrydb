//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLTableDescr.cpp
//
//	@doc:
//		Implementation of DXL table descriptors
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

#define GPDXL_DEFAULT_USERID 0

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::CDXLTableDescr
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLTableDescr::CDXLTableDescr
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	ULONG ulExecuteAsUser
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pdrgdxlcd(NULL),
	m_ulExecuteAsUser(ulExecuteAsUser)
{
	GPOS_ASSERT(NULL != m_pmdname);
	m_pdrgdxlcd = GPOS_NEW(pmp) DrgPdxlcd(pmp);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::~CDXLTableDescr
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLTableDescr::~CDXLTableDescr()
{
	m_pmdid->Release();
	GPOS_DELETE(m_pmdname);
	CRefCount::SafeRelease(m_pdrgdxlcd);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::Pmdid
//
//	@doc:
//		Return the metadata id for the table
//
//---------------------------------------------------------------------------
IMDId *
CDXLTableDescr::Pmdid() const
{	
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::Pmdname
//
//	@doc:
//		Return table name
//
//---------------------------------------------------------------------------
const CMDName *
CDXLTableDescr::Pmdname() const
{
	return m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::UlArity
//
//	@doc:
//		Return number of columns in the table
//
//---------------------------------------------------------------------------
ULONG
CDXLTableDescr::UlArity() const
{
	return (m_pdrgdxlcd == NULL) ? 0 : m_pdrgdxlcd->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::UlExecuteAsUser
//
//	@doc:
//		Id of the user the table needs to be accessed with
//
//---------------------------------------------------------------------------
ULONG
CDXLTableDescr::UlExecuteAsUser() const
{
	return m_ulExecuteAsUser;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::SetColumnDescriptors
//
//	@doc:
//		Set the list of column descriptors
//
//---------------------------------------------------------------------------
void
CDXLTableDescr::SetColumnDescriptors
	(
	DrgPdxlcd *pdrgpdxlcd
	)
{
	CRefCount::SafeRelease(m_pdrgdxlcd);
	m_pdrgdxlcd = pdrgpdxlcd;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::AddColumnDescr
//
//	@doc:
//		Add a column to the list of column descriptors
//
//---------------------------------------------------------------------------
void
CDXLTableDescr::AddColumnDescr
	(
	CDXLColDescr *pdxlcr
	)
{
	GPOS_ASSERT(NULL != m_pdrgdxlcd);
	GPOS_ASSERT(NULL != pdxlcr);
	m_pdrgdxlcd->Append(pdxlcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::Pdxlcd
//
//	@doc:
//		Get the column descriptor at the specified position from the col descr list
//
//---------------------------------------------------------------------------
const CDXLColDescr *
CDXLTableDescr::Pdxlcd
	(
	ULONG ul
	)
	const
{
	GPOS_ASSERT(ul < UlArity());
	
	return (*m_pdrgdxlcd)[ul];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::SerializeMDId
//
//	@doc:
//		Serialize the metadata id in DXL format
//
//---------------------------------------------------------------------------
void
CDXLTableDescr::SerializeMDId
	(
	CXMLSerializer *pxmlser
	) const
{
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLTableDescr::SerializeToDXL
//
//	@doc:
//		Serialize table descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLTableDescr::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenTableDescr));
	
	SerializeMDId(pxmlser);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTableName), m_pmdname->Pstr());
	
	if (GPDXL_DEFAULT_USERID != m_ulExecuteAsUser)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenExecuteAsUser), m_ulExecuteAsUser);
	}
	
	// serialize columns
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));
	GPOS_ASSERT(NULL != m_pdrgdxlcd);
	
	const ULONG ulArity = UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLColDescr *pdxlcd = (*m_pdrgdxlcd)[ul];
		pdxlcd->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenTableDescr));
}

// EOF
