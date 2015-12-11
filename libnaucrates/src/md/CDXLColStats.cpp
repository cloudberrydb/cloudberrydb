//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLColStats.cpp
//
//	@doc:
//		Implementation of the class for representing column stats in DXL
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CDXLColStats.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpos/common/CAutoRef.h"

#include "naucrates/statistics/CStatistics.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::CDXLColStats
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLColStats::CDXLColStats
	(
	IMemoryPool *pmp,
	CMDIdColStats *pmdidColStats,
	CMDName *pmdname,
	CDouble dWidth,
	CDouble dNullFreq,
	CDouble dDistinctRemain,
	CDouble dFreqRemain,
	DrgPdxlbucket *pdrgpdxlbucket,
	BOOL fColStatsMissing
	)
	:
	m_pmp(pmp),
	m_pmdidColStats(pmdidColStats),
	m_pmdname(pmdname),
	m_dWidth(dWidth),
	m_dNullFreq(dNullFreq),
	m_dDistinctRemain(dDistinctRemain),
	m_dFreqRemain(dFreqRemain),
	m_pdrgpdxlbucket(pdrgpdxlbucket),
	m_fColStatsMissing(fColStatsMissing)
{
	GPOS_ASSERT(pmdidColStats->FValid());
	GPOS_ASSERT(NULL != pdrgpdxlbucket);
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::~CDXLColStats
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLColStats::~CDXLColStats()
{
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdidColStats->Release();
	m_pdrgpdxlbucket->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::Pmdid
//
//	@doc:
//		Returns the metadata id of this column stats object
//
//---------------------------------------------------------------------------
IMDId *
CDXLColStats::Pmdid() const
{
	return m_pmdidColStats;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::Mdname
//
//	@doc:
//		Returns the name of this column
//
//---------------------------------------------------------------------------
CMDName
CDXLColStats::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::Pstr
//
//	@doc:
//		Returns the DXL string for this object
//
//---------------------------------------------------------------------------
const CWStringDynamic *
CDXLColStats::Pstr() const
{
	return m_pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::UlBuckets
//
//	@doc:
//		Returns the number of buckets in the histogram
//
//---------------------------------------------------------------------------
ULONG
CDXLColStats::UlBuckets() const
{
	return m_pdrgpdxlbucket->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::Pdxlbucket
//
//	@doc:
//		Returns the bucket at the given position
//
//---------------------------------------------------------------------------
const CDXLBucket *
CDXLColStats::Pdxlbucket
	(
	ULONG ul
	) 
	const
{
	return (*m_pdrgpdxlbucket)[ul];
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::Serialize
//
//	@doc:
//		Serialize column stats in DXL format
//
//---------------------------------------------------------------------------
void
CDXLColStats::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumnStats));
	
	m_pmdidColStats->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWidth), m_dWidth);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColNullFreq), m_dNullFreq);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColNdvRemain), m_dDistinctRemain);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColFreqRemain), m_dFreqRemain);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColStatsMissing), m_fColStatsMissing);

	GPOS_CHECK_ABORT;

	ULONG ulBuckets = UlBuckets();
	for (ULONG ul = 0; ul < ulBuckets; ul++)
	{
		const CDXLBucket *pdxlbucket = Pdxlbucket(ul);
		pdxlbucket->Serialize(pxmlser);

		GPOS_CHECK_ABORT;
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenColumnStats));
}



#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::DebugPrint
//
//	@doc:
//		Dbug print of the column stats object
//
//---------------------------------------------------------------------------
void
CDXLColStats::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Column id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Column name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	
	for (ULONG ul = 0; ul < UlBuckets(); ul++)
	{
		const CDXLBucket *pdxlbucket = Pdxlbucket(ul);
		pdxlbucket->DebugPrint(os);
	}
}

#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CDXLColStats::PdxlcolstatsDummy
//
//	@doc:
//		Dummy statistics
//
//---------------------------------------------------------------------------
CDXLColStats *
CDXLColStats::PdxlcolstatsDummy
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	CDouble dWidth
	)
{
	CMDIdColStats *pmdidColStats = CMDIdColStats::PmdidConvert(pmdid);

	CAutoRef<DrgPdxlbucket> a_pdrgpdxlbucket;
	a_pdrgpdxlbucket = GPOS_NEW(pmp) DrgPdxlbucket(pmp);
	CAutoRef<CDXLColStats> a_pdxlcolstats;
	a_pdxlcolstats = GPOS_NEW(pmp) CDXLColStats
					(
					pmp,
					pmdidColStats,
					pmdname,
					dWidth,
					CHistogram::DDefaultNullFreq,
					CHistogram::DDefaultNDVRemain,
					CHistogram::DDefaultNDVFreqRemain,
					a_pdrgpdxlbucket.Pt(),
					true /* fColStatsMissing */
					);
	a_pdrgpdxlbucket.PtReset();
	return a_pdxlcolstats.PtReset();
}

// EOF

