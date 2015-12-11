//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLStatsDerivedRelation.cpp
//
//	@doc:
//		Implementation of the class for representing DXL derived relation statistics
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"
#include "naucrates/md/CDXLStatsDerivedRelation.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::CDXLStatsDerivedRelation
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLStatsDerivedRelation::CDXLStatsDerivedRelation
	(
	CDouble dRows,
	BOOL fEmpty,
	DrgPdxlstatsdercol *pdrgpdxldercolstat
	)
	:
	m_dRows(dRows),
	m_fEmpty(fEmpty),
	m_pdrgpdxlstatsdercol(pdrgpdxldercolstat)
{
	GPOS_ASSERT(NULL != pdrgpdxldercolstat);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::~CDXLStatsDerivedRelation
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLStatsDerivedRelation::~CDXLStatsDerivedRelation()
{
	m_pdrgpdxlstatsdercol->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::Pdrgpdxlstatsdercol
//
//	@doc:
//		Returns the array of derived columns stats
//
//---------------------------------------------------------------------------
const DrgPdxlstatsdercol *
CDXLStatsDerivedRelation::Pdrgpdxlstatsdercol() const
{
	return m_pdrgpdxlstatsdercol;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::Serialize
//
//	@doc:
//		Serialize bucket in DXL format
//
//---------------------------------------------------------------------------
void
CDXLStatsDerivedRelation::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenStatsDerivedRelation));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRows), m_dRows);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenEmptyRelation), m_fEmpty);

	const ULONG ulColStats = m_pdrgpdxlstatsdercol->UlLength();
	for (ULONG ul = 0; ul < ulColStats; ul++)
	{
		GPOS_CHECK_ABORT;

		CDXLStatsDerivedColumn *pdxldercolstats = (*m_pdrgpdxlstatsdercol)[ul];
		pdxldercolstats->Serialize(pxmlser);

		GPOS_CHECK_ABORT;
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenStatsDerivedRelation));
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLStatsDerivedRelation::DebugPrint
//
//	@doc:
//		Debug print of the bucket object
//
//---------------------------------------------------------------------------
void
CDXLStatsDerivedRelation::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Rows: " << DRows() << std::endl;

	os << "Empty: " << FEmpty() << std::endl;
}

#endif // GPOS_DEBUG

// EOF

