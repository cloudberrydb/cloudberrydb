//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalTableScan.cpp
//
//	@doc:
//		Implementation of DXL physical table scan operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalTableScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::CDXLPhysicalTableScan
//
//	@doc:
//		Construct a table scan node with uninitialized table descriptor
//
//---------------------------------------------------------------------------
CDXLPhysicalTableScan::CDXLPhysicalTableScan
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysical(pmp),
	m_pdxltabdesc(NULL)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::CDXLPhysicalTableScan
//
//	@doc:
//		Construct a table scan node given its table descriptor
//
//---------------------------------------------------------------------------
CDXLPhysicalTableScan::CDXLPhysicalTableScan
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc
	)
	:CDXLPhysical(pmp),
	 m_pdxltabdesc(pdxltabdesc)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::~CDXLPhysicalTableScan
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalTableScan::~CDXLPhysicalTableScan()
{
	CRefCount::SafeRelease(m_pdxltabdesc);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::SetTableDescriptor
//
//	@doc:
//		Set table descriptor
//
//---------------------------------------------------------------------------
void
CDXLPhysicalTableScan::SetTableDescriptor
	(
	CDXLTableDescr *pdxltabdesc
	)
{
	// allow setting table descriptor only once
	GPOS_ASSERT (NULL == m_pdxltabdesc);
	
	m_pdxltabdesc = pdxltabdesc;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalTableScan::Edxlop() const
{
	return EdxlopPhysicalTableScan;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalTableScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalTableScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::Pdxltabdesc
//
//	@doc:
//		Table descriptor for the table scan
//
//---------------------------------------------------------------------------
const CDXLTableDescr *
CDXLPhysicalTableScan::Pdxltabdesc()
{
	return m_pdxltabdesc;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalTableScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);
	
	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(pxmlser);
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);		
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalTableScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLPhysicalTableScan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	// assert proj list and filter are valid
	CDXLPhysical::AssertValid(pdxln, fValidateChildren);
	
	// table scan has only 2 children
	GPOS_ASSERT(2 == pdxln->UlArity());
	
	// assert validity of table descriptor
	GPOS_ASSERT(NULL != m_pdxltabdesc);
	GPOS_ASSERT(NULL != m_pdxltabdesc->Pmdname());
	GPOS_ASSERT(m_pdxltabdesc->Pmdname()->Pstr()->FValid());
}
#endif // GPOS_DEBUG

// EOF
