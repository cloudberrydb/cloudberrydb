//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalSharedScan.cpp
//
//	@doc:
//		Implementation of DXL physical shared scan operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalSharedScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::CDXLPhysicalSharedScan
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSharedScan::CDXLPhysicalSharedScan
	(
	IMemoryPool *pmp,
	CDXLSpoolInfo *pspoolinfo
	)
	:
	CDXLPhysical(pmp),
	m_pspoolinfo(pspoolinfo)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::~CDXLPhysicalSharedScan
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLPhysicalSharedScan::~CDXLPhysicalSharedScan()
{
	GPOS_DELETE(m_pspoolinfo);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalSharedScan::Edxlop() const
{
	return EdxlopPhysicalSharedScan;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalSharedScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalSharedScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::Pspoolinfo
//
//	@doc:
//		Returns the spooling info for this shared scan operator
//
//---------------------------------------------------------------------------
const CDXLSpoolInfo *
CDXLPhysicalSharedScan::Pspoolinfo() const
{
	return m_pspoolinfo;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSharedScan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	// serialize spool info as attributes of the shared scan element
	m_pspoolinfo->SerializeToDXL(pxmlser);
	
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalSharedScan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalSharedScan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{

	GPOS_ASSERT(EdxlshscanIndexSentinel >= pdxln->UlArity());

	if (EdxlshscanIndexSentinel == pdxln->UlArity())
	{
		CDXLNode *pdxlnChild = (*pdxln)[EdxlshscanIndexChild];
		GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}

}
#endif // GPOS_DEBUG

// EOF
