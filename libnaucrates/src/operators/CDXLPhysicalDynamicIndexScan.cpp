//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDynamicIndexScan.cpp
//
//	@doc:
//		Implementation of DXL physical dynamic index scan operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalDynamicIndexScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::CDXLPhysicalDynamicIndexScan
//
//	@doc:
//		Construct an index scan node given its table descriptor,
//		index descriptor and filter conditions on the index
//
//---------------------------------------------------------------------------
CDXLPhysicalDynamicIndexScan::CDXLPhysicalDynamicIndexScan
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	ULONG ulPartIndexId,
	ULONG ulPartIndexIdPrintable,
	CDXLIndexDescr *pdxlid,
	EdxlIndexScanDirection edxlisd
	)
	:
	CDXLPhysical(pmp),
	m_pdxltabdesc(pdxltabdesc),
	m_ulPartIndexId(ulPartIndexId),
	m_ulPartIndexIdPrintable(ulPartIndexIdPrintable),
	m_pdxlid(pdxlid),
	m_edxlisd(edxlisd)
{
	GPOS_ASSERT(NULL != m_pdxltabdesc);
	GPOS_ASSERT(NULL != m_pdxlid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::~CDXLPhysicalDynamicIndexScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalDynamicIndexScan::~CDXLPhysicalDynamicIndexScan()
{
	m_pdxlid->Release();
	m_pdxltabdesc->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalDynamicIndexScan::Edxlop() const
{
	return EdxlopPhysicalDynamicIndexScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalDynamicIndexScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalDynamicIndexScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::Pdxlid
//
//	@doc:
//		Index descriptor for the index scan
//
//---------------------------------------------------------------------------
const CDXLIndexDescr *
CDXLPhysicalDynamicIndexScan::Pdxlid() const
{
	return m_pdxlid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::EdxlScanDirection
//
//	@doc:
//		Return the scan direction of the index
//
//---------------------------------------------------------------------------
EdxlIndexScanDirection
CDXLPhysicalDynamicIndexScan::EdxlScanDirection() const
{
	return m_edxlisd;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::Pdxltabdesc
//
//	@doc:
//		Return the associated table descriptor
//
//---------------------------------------------------------------------------
const CDXLTableDescr *
CDXLPhysicalDynamicIndexScan::Pdxltabdesc() const
{
	return m_pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::UlPartIndexId
//
//	@doc:
//		Part index id
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalDynamicIndexScan::UlPartIndexId() const
{
	return m_ulPartIndexId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::UlPartIndexIdPrintable
//
//	@doc:
//		Printable partition index id
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalDynamicIndexScan::UlPartIndexIdPrintable() const
{
	return m_ulPartIndexIdPrintable;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDynamicIndexScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute
				(
				CDXLTokens::PstrToken(EdxltokenIndexScanDirection),
				CDXLOperator::PstrIndexScanDirection(m_edxlisd)
				);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartIndexId), m_ulPartIndexId);
	if (m_ulPartIndexIdPrintable != m_ulPartIndexId)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartIndexIdPrintable), m_ulPartIndexIdPrintable);
	}

	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	// serialize index descriptor
	m_pdxlid->SerializeToDXL(pxmlser);

	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDynamicIndexScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDynamicIndexScan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);

	// index scan has only 3 children
	GPOS_ASSERT(3 == pdxln->UlArity());

	// assert validity of the index descriptor
	GPOS_ASSERT(NULL != m_pdxlid);
	GPOS_ASSERT(NULL != m_pdxlid->Pmdname());
	GPOS_ASSERT(m_pdxlid->Pmdname()->Pstr()->FValid());

	// assert validity of the table descriptor
	GPOS_ASSERT(NULL != m_pdxltabdesc);
	GPOS_ASSERT(NULL != m_pdxltabdesc->Pmdname());
	GPOS_ASSERT(m_pdxltabdesc->Pmdname()->Pstr()->FValid());

	CDXLNode *pdxlnIndexFilter = (*pdxln)[EdxldisIndexFilter];
	CDXLNode *pdxlnIndexConds = (*pdxln)[EdxldisIndexCondition];

	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarIndexCondList == pdxlnIndexConds->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxlopScalarFilter == pdxlnIndexFilter->Pdxlop()->Edxlop());

	if (fValidateChildren)
	{
		pdxlnIndexConds->Pdxlop()->AssertValid(pdxlnIndexConds, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
