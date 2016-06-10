//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLBucket.cpp
//
//	@doc:
//		Implementation of the class for representing buckets in DXL column stats
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CDXLBucket.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::CDXLBucket
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLBucket::CDXLBucket
	(
	CDXLDatum *pdxldatumLower,
	CDXLDatum *pdxldatumUpper,
	BOOL fLowerClosed,
	BOOL fUpperClosed,
	CDouble dFrequency,
	CDouble dDistinct
	)
	:
	m_pdxldatumLower(pdxldatumLower),
	m_pdxldatumUpper(pdxldatumUpper),
	m_fLowerClosed(fLowerClosed),
	m_fUpperClosed(fUpperClosed),
	m_dFrequency(dFrequency),
	m_dDistinct(dDistinct)
{
	GPOS_ASSERT(NULL != pdxldatumLower);
	GPOS_ASSERT(NULL != pdxldatumUpper);
	GPOS_ASSERT(m_dFrequency >= 0.0 && m_dFrequency <= 1.0);
	GPOS_ASSERT(m_dDistinct >= 0);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::~CDXLBucket
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLBucket::~CDXLBucket()
{
	m_pdxldatumLower->Release();
	m_pdxldatumUpper->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::PdxldatumLower
//
//	@doc:
//		Returns the lower bound for the bucket
//
//---------------------------------------------------------------------------
const CDXLDatum *
CDXLBucket::PdxldatumLower() const
{
	return m_pdxldatumLower;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::PdxldatumUpper
//
//	@doc:
//		Returns the upper bound for the bucket
//
//---------------------------------------------------------------------------
const CDXLDatum *
CDXLBucket::PdxldatumUpper() const
{
	return m_pdxldatumUpper;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::DFrequency
//
//	@doc:
//		Returns the frequency for this bucket
//
//---------------------------------------------------------------------------
CDouble
CDXLBucket::DFrequency() const
{
	return m_dFrequency;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::DDistinct
//
//	@doc:
//		Returns the number of distinct in this bucket
//
//---------------------------------------------------------------------------
CDouble
CDXLBucket::DDistinct() const
{
	return m_dDistinct;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::Serialize
//
//	@doc:
//		Serialize bucket in DXL format
//
//---------------------------------------------------------------------------
void
CDXLBucket::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumnStatsBucket));
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenStatsFrequency), m_dFrequency);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenStatsDistinct), m_dDistinct);
	
	SerializeBoundaryValue(pxmlser, CDXLTokens::PstrToken(EdxltokenStatsBucketLowerBound), m_pdxldatumLower, m_fLowerClosed);
	SerializeBoundaryValue(pxmlser, CDXLTokens::PstrToken(EdxltokenStatsBucketUpperBound), m_pdxldatumUpper, m_fUpperClosed);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumnStatsBucket));

	GPOS_CHECK_ABORT;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::Serialize
//
//	@doc:
//		Serialize the bucket boundary
//
//---------------------------------------------------------------------------
void
CDXLBucket::SerializeBoundaryValue
	(
	CXMLSerializer *pxmlser,
	const CWStringConst *pstrElem,
	CDXLDatum *pdxldatum,
	BOOL fBoundClosed
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElem);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenStatsBoundClosed), fBoundClosed);
	pdxldatum->Serialize(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElem);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLBucket::DebugPrint
//
//	@doc:
//		Debug print of the bucket object
//
//---------------------------------------------------------------------------
void
CDXLBucket::DebugPrint
	(
	IOstream & //os
	)
	const
{
	// TODO:  - Feb 13, 2012; implement
}

#endif // GPOS_DEBUG

// EOF

