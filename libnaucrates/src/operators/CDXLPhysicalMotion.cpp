//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalMotion.cpp
//
//	@doc:
//		Implementation of DXL physical motion operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalMotion.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::CDXLPhysicalMotion
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalMotion::CDXLPhysicalMotion
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysical(pmp),
	m_pdrgpiInputSegIds(NULL),
	m_pdrgpiOutputSegIds(NULL)
{
}

CDXLPhysicalMotion::~CDXLPhysicalMotion()
{
	CRefCount::SafeRelease(m_pdrgpiInputSegIds);
	CRefCount::SafeRelease(m_pdrgpiOutputSegIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::PdrgpiInputSegIds
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
const DrgPi *
CDXLPhysicalMotion::PdrgpiInputSegIds() const
{
	return m_pdrgpiInputSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::PdrgpiOutputSegIds
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
const DrgPi *
CDXLPhysicalMotion::PdrgpiOutputSegIds() const
{
	return m_pdrgpiOutputSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::SetInputSegIds
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMotion::SetInputSegIds(DrgPi *pdrgpiInputSegIds)
{
	GPOS_ASSERT(NULL == m_pdrgpiInputSegIds);
	GPOS_ASSERT(NULL != pdrgpiInputSegIds);
	m_pdrgpiInputSegIds = pdrgpiInputSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::SetOutputSegIds
//
//	@doc:
//		
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMotion::SetOutputSegIds(DrgPi *pdrgpiOutputSegIds)
{
	GPOS_ASSERT(NULL == m_pdrgpiOutputSegIds);
	GPOS_ASSERT(NULL != pdrgpiOutputSegIds);
	m_pdrgpiOutputSegIds = pdrgpiOutputSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::SetSegmentInfo
//
//	@doc:
//		Set input and output segment information
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMotion::SetSegmentInfo
	(
	DrgPi *pdrgpiInputSegIds, 
	DrgPi *pdrgpiOutputSegIds
	)
{
	GPOS_ASSERT(NULL == m_pdrgpiOutputSegIds && NULL == m_pdrgpiInputSegIds);
	GPOS_ASSERT(NULL != pdrgpiOutputSegIds && NULL != pdrgpiInputSegIds);
	
	m_pdrgpiInputSegIds = pdrgpiInputSegIds;
	m_pdrgpiOutputSegIds = pdrgpiOutputSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::PstrSegIds
//
//	@doc:
//		Serialize the array of segment ids into a comma-separated string		
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLPhysicalMotion::PstrSegIds(const DrgPi *pdrgpi) const
{
	GPOS_ASSERT(pdrgpi != NULL && 0 < pdrgpi->UlLength());
	
	CWStringDynamic *pstr = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);
	
	ULONG ulNumSegments = pdrgpi->UlLength();
	for (ULONG ul = 0; ul < ulNumSegments; ul++)
	{
		INT iSegId = *((*pdrgpi)[ul]);
		if (ul == ulNumSegments - 1)
		{
			// last element: do not print a comma
			pstr->AppendFormat(GPOS_WSZ_LIT("%d"), iSegId);
		}
		else
		{
			pstr->AppendFormat(GPOS_WSZ_LIT("%d%ls"), iSegId, CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
		}
	}
	
	return pstr;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::PstrInputSegIds
//
//	@doc:
//		Serialize the array of input segment ids into a comma-separated string		
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLPhysicalMotion::PstrInputSegIds() const
{	
	return PstrSegIds(m_pdrgpiInputSegIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::PstrOutputSegIds
//
//	@doc:
//		Serialize the array of output segment ids into a comma-separated string		
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLPhysicalMotion::PstrOutputSegIds() const
{	
	return PstrSegIds(m_pdrgpiOutputSegIds);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMotion::SerializeSegmentInfoToDXL
//
//	@doc:
//		Serialize the array of input and output segment ids in DXL format		
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMotion::SerializeSegmentInfoToDXL
	(
	CXMLSerializer *pxmlser
	) const
{
	CWStringDynamic *pstrInputSegIds = PstrInputSegIds();
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInputSegments), pstrInputSegIds);
	
	CWStringDynamic *pstrOutputSegIds = PstrOutputSegIds();
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOutputSegments), pstrOutputSegIds);
		
	GPOS_DELETE(pstrInputSegIds);
	GPOS_DELETE(pstrOutputSegIds);
}


// EOF
