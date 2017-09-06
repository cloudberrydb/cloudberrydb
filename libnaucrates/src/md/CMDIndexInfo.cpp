//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software Inc.
//
//	@filename:
//		CMDIndexInfo.cpp
//
//	@doc:
//		Implementation of the class for representing indexinfo
//---------------------------------------------------------------------------

#include "naucrates/md/CMDIndexInfo.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;
using namespace gpmd;

// ctor
CMDIndexInfo::CMDIndexInfo
	(
	IMDId *pmdid,
	BOOL fPartial
	)
	:
	m_pmdid(pmdid),
	m_fPartial(fPartial)
{
	GPOS_ASSERT(pmdid->FValid());
}

// dtor
CMDIndexInfo::~CMDIndexInfo()
{
	m_pmdid->Release();
}

// returns the metadata id of this index
IMDId *
CMDIndexInfo::Pmdid() const
{
	return m_pmdid;
}

// is the index partial
BOOL
CMDIndexInfo::FPartial() const
{
	return m_fPartial;
}

// serialize indexinfo in DXL format
void
CMDIndexInfo::Serialize
	(
	gpdxl::CXMLSerializer *pxmlser
	) const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenIndexInfo));

	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIndexPartial), m_fPartial);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenIndexInfo));
}

#ifdef GPOS_DEBUG
// prints a indexinfo to the provided output
void
CMDIndexInfo::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Index id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	os << "Is partial index: " << m_fPartial << std::endl;
}

#endif // GPOS_DEBUG
