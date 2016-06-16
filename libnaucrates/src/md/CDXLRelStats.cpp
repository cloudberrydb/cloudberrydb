//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLRelStats.cpp
//
//	@doc:
//		Implementation of the class for representing relation stats in DXL
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CDXLRelStats.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "gpos/common/CAutoP.h"
#include "gpos/common/CAutoRef.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::CDXLRelStats
//
//	@doc:
//		Constructs a metadata relation
//
//---------------------------------------------------------------------------
CDXLRelStats::CDXLRelStats
	(
	IMemoryPool *pmp,
	CMDIdRelStats *pmdidRelStats,
	CMDName *pmdname,
	CDouble dRows,
	BOOL fEmpty
	)
	:
	m_pmp(pmp),
	m_pmdidRelStats(pmdidRelStats),
	m_pmdname(pmdname),
	m_dRows(dRows),
	m_fEmpty(fEmpty)
{
	GPOS_ASSERT(pmdidRelStats->FValid());
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::~CDXLRelStats
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLRelStats::~CDXLRelStats()
{
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pmdidRelStats->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::Pmdid
//
//	@doc:
//		Returns the metadata id of this relation stats object
//
//---------------------------------------------------------------------------
IMDId *
CDXLRelStats::Pmdid() const
{
	return m_pmdidRelStats;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::Mdname
//
//	@doc:
//		Returns the name of this relation
//
//---------------------------------------------------------------------------
CMDName
CDXLRelStats::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::Pstr
//
//	@doc:
//		Returns the DXL string for this object
//
//---------------------------------------------------------------------------
const CWStringDynamic *
CDXLRelStats::Pstr() const
{
	return m_pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::DRows
//
//	@doc:
//		Returns the number of rows
//
//---------------------------------------------------------------------------
CDouble
CDXLRelStats::DRows() const
{
	return m_dRows;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::Serialize
//
//	@doc:
//		Serialize relation stats in DXL format
//
//---------------------------------------------------------------------------
void
CDXLRelStats::Serialize
	(
	CXMLSerializer *pxmlser
	) const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenRelationStats));
	
	m_pmdidRelStats->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRows), m_dRows);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenEmptyRelation), m_fEmpty);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenRelationStats));

	GPOS_CHECK_ABORT;
}



#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CDXLRelStats::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Relation id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Relation name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	
	os << "Rows: " << DRows() << std::endl;

	os << "Empty: " << FEmpty() << std::endl;
}

#endif // GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CDXLRelStats::PdxlrelstatsDummy
//
//	@doc:
//		Dummy relation stats
//
//---------------------------------------------------------------------------
CDXLRelStats *
CDXLRelStats::PdxlrelstatsDummy
	(
	IMemoryPool *pmp,
	IMDId *pmdid
	)
{
	CMDIdRelStats *pmdidRelStats = CMDIdRelStats::PmdidConvert(pmdid);
	CAutoP<CWStringDynamic> a_pstr;
	a_pstr = GPOS_NEW(pmp) CWStringDynamic(pmp, pmdidRelStats->Wsz());
	CAutoP<CMDName> a_pmdname;
	a_pmdname = GPOS_NEW(pmp) CMDName(pmp, a_pstr.Pt());
	CAutoRef<CDXLRelStats> a_pdxlrelstats;
	a_pdxlrelstats = GPOS_NEW(pmp) CDXLRelStats(pmp, pmdidRelStats, a_pmdname.Pt(), CStatistics::DDefaultColumnWidth, false /* fEmpty */);
	a_pmdname.PtReset();
	return a_pdxlrelstats.PtReset();
}

// EOF

