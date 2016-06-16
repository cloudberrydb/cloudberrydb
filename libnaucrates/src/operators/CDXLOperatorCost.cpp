//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLOperatorCost.cpp
//
//	@doc:
//		Class for representing cost estimates in physical operators.
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLOperatorCost.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

CDXLOperatorCost::CDXLOperatorCost
	(
	CWStringDynamic *pstrStartupCost,
	CWStringDynamic *pstrTotalCost,
	CWStringDynamic *pstrRows,
	CWStringDynamic *pstrWidth
	)
	:
	m_pstrStartupCost(pstrStartupCost),
	m_pstrTotalCost(pstrTotalCost),
	m_pstrRows(pstrRows),
	m_pstrWidth(pstrWidth)
{
}

CDXLOperatorCost::~CDXLOperatorCost()
{
	GPOS_DELETE(m_pstrStartupCost);
	GPOS_DELETE(m_pstrTotalCost);
	GPOS_DELETE(m_pstrRows);
	GPOS_DELETE(m_pstrWidth);
}

const CWStringDynamic *
CDXLOperatorCost::PstrStartupCost() const
{
	return m_pstrStartupCost;
}

const CWStringDynamic *
CDXLOperatorCost::PstrTotalCost() const
{
	return m_pstrTotalCost;
}

const CWStringDynamic *
CDXLOperatorCost::PstrRows() const
{
	return m_pstrRows;
}

const CWStringDynamic *
CDXLOperatorCost::PstrWidth() const
{
	return m_pstrWidth;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorCost::SetRows
//
//	@doc:
//		Set the number of rows
//
//---------------------------------------------------------------------------
void
CDXLOperatorCost::SetRows
	(
	CWStringDynamic *pstr
	)
{
	GPOS_ASSERT(NULL != pstr);
	GPOS_DELETE(m_pstrRows);
	m_pstrRows = pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorCost::SetCost
//
//	@doc:
//		Set the total cost
//
//---------------------------------------------------------------------------
void
CDXLOperatorCost::SetCost
	(
	CWStringDynamic *pstr
	)
{
	GPOS_ASSERT(NULL != pstr);
	GPOS_DELETE(m_pstrTotalCost);
	m_pstrTotalCost = pstr;
}

void
CDXLOperatorCost::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCost));
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenStartupCost), m_pstrStartupCost);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTotalCost), m_pstrTotalCost);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRows), m_pstrRows);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWidth), m_pstrWidth);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCost));
}

// EOF
