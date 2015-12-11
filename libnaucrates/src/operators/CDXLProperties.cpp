//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLProperties.cpp
//
//	@doc:
//		Implementation of properties of DXL operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLProperties.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::CDXLProperties
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLProperties::CDXLProperties()
	:
	m_pdxlstatsderrel(NULL)
{}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::~CDXLProperties
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLProperties::~CDXLProperties()
{
	CRefCount::SafeRelease(m_pdxlstatsderrel);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::SetStats
//
//	@doc:
//		Set operator properties
//
//---------------------------------------------------------------------------
void
CDXLProperties::SetStats
	(
	CDXLStatsDerivedRelation *pdxlstatsderrel
	)
{
	// allow setting properties only once
	GPOS_ASSERT(NULL == m_pdxlstatsderrel);
	GPOS_ASSERT(NULL != pdxlstatsderrel);
	m_pdxlstatsderrel = pdxlstatsderrel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::Pdxlstatsderrel
//
//	@doc:
//		Return operator's statistical information
//
//---------------------------------------------------------------------------
const CDXLStatsDerivedRelation *
CDXLProperties::Pdxlstatsderrel() const
{
	return m_pdxlstatsderrel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::SerializePropertiesToDXL
//
//	@doc:
//		Serialize operator statistics in DXL format
//
//---------------------------------------------------------------------------
void
CDXLProperties::SerializePropertiesToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	SerializeStatsToDXL(pxmlser);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLProperties::SerializeStatsToDXL
//
//	@doc:
//		Serialize operator statistics in DXL format
//
//---------------------------------------------------------------------------
void
CDXLProperties::SerializeStatsToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	if (NULL != m_pdxlstatsderrel)
	{
		m_pdxlstatsderrel->Serialize(pxmlser);
	}
}

// EOF
