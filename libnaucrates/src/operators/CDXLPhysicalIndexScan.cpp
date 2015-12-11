//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalIndexScan.cpp
//
//	@doc:
//		Implementation of DXL physical index scan operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalIndexScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::CDXLPhysicalIndexScan
//
//	@doc:
//		Construct an index scan node given its table descriptor,
//		index descriptor and filter conditions on the index
//
//---------------------------------------------------------------------------
CDXLPhysicalIndexScan::CDXLPhysicalIndexScan
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	CDXLIndexDescr *pdxlid,
	EdxlIndexScanDirection edxlisd
	)
	:
	CDXLPhysical(pmp),
	m_pdxltabdesc(pdxltabdesc),
	m_pdxlid(pdxlid),
	m_edxlisd(edxlisd)
{
	GPOS_ASSERT(NULL != m_pdxltabdesc);
	GPOS_ASSERT(NULL != m_pdxlid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::~CDXLPhysicalIndexScan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalIndexScan::~CDXLPhysicalIndexScan()
{
	m_pdxlid->Release();
	m_pdxltabdesc->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalIndexScan::Edxlop() const
{
	return EdxlopPhysicalIndexScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalIndexScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalIndexScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::Pdxlid
//
//	@doc:
//		Index descriptor for the index scan
//
//---------------------------------------------------------------------------
const CDXLIndexDescr *
CDXLPhysicalIndexScan::Pdxlid() const
{
	return m_pdxlid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::EdxlScanDirection
//
//	@doc:
//		Return the scan direction of the index
//
//---------------------------------------------------------------------------
EdxlIndexScanDirection
CDXLPhysicalIndexScan::EdxlScanDirection() const
{
	return m_edxlisd;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::Pdxltabdesc
//
//	@doc:
//		Return the associated table descriptor
//
//---------------------------------------------------------------------------
const CDXLTableDescr *
CDXLPhysicalIndexScan::Pdxltabdesc() const
{
	return m_pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalIndexScan::SerializeToDXL
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
//		CDXLPhysicalIndexScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalIndexScan::AssertValid
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

	CDXLNode *pdxlnIndexConds = (*pdxln)[EdxlisIndexCondition];

	// assert children are of right type (physical/scalar)
	GPOS_ASSERT(EdxlopScalarIndexCondList == pdxlnIndexConds->Pdxlop()->Edxlop());

	if (fValidateChildren)
	{
		pdxlnIndexConds->Pdxlop()->AssertValid(pdxlnIndexConds, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
