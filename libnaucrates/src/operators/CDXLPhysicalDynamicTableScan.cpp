//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDynamicTableScan.cpp
//
//	@doc:
//		Implementation of DXL physical dynamic table scan operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalDynamicTableScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::CDXLPhysicalDynamicTableScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalDynamicTableScan::CDXLPhysicalDynamicTableScan
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	ULONG ulPartIndexId,
	ULONG ulPartIndexIdPrintable
	)
	:
	CDXLPhysical(pmp),
	m_pdxltabdesc(pdxltabdesc),
	m_ulPartIndexId(ulPartIndexId),
	m_ulPartIndexIdPrintable(ulPartIndexIdPrintable)
{
	GPOS_ASSERT(NULL != pdxltabdesc);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::~CDXLPhysicalDynamicTableScan
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalDynamicTableScan::~CDXLPhysicalDynamicTableScan()
{
	m_pdxltabdesc->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalDynamicTableScan::Edxlop() const
{
	return EdxlopPhysicalDynamicTableScan;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalDynamicTableScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalDynamicTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::Pdxltabdesc
//
//	@doc:
//		Table descriptor for the table scan
//
//---------------------------------------------------------------------------
const CDXLTableDescr *
CDXLPhysicalDynamicTableScan::Pdxltabdesc() const
{
	return m_pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::UlPartIndexId
//
//	@doc:
//		Id of partition index
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalDynamicTableScan::UlPartIndexId() const
{
	return m_ulPartIndexId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::UlPartIndexIdPrintable
//
//	@doc:
//		Printable partition index id
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalDynamicTableScan::UlPartIndexIdPrintable() const
{
	return m_ulPartIndexIdPrintable;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDynamicTableScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartIndexId), m_ulPartIndexId);
	if (m_ulPartIndexIdPrintable != m_ulPartIndexId)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartIndexIdPrintable), m_ulPartIndexIdPrintable);
	}
	pdxln->SerializePropertiesToDXL(pxmlser);
	pdxln->SerializeChildrenToDXL(pxmlser);
	m_pdxltabdesc->SerializeToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicTableScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDynamicTableScan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	) 
	const
{
	GPOS_ASSERT(2 == pdxln->UlArity());
	
	// assert validity of table descriptor
	GPOS_ASSERT(NULL != m_pdxltabdesc);
	GPOS_ASSERT(NULL != m_pdxltabdesc->Pmdname());
	GPOS_ASSERT(m_pdxltabdesc->Pmdname()->Pstr()->FValid());
}
#endif // GPOS_DEBUG

// EOF
