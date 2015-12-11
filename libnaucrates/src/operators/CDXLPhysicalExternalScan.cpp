//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 Pivotal, Inc.
//
//	@filename:
//		CDXLPhysicalExternalScan.cpp
//
//	@doc:
//		Implementation of DXL physical external scan operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalExternalScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalExternalScan::CDXLPhysicalExternalScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalExternalScan::CDXLPhysicalExternalScan
	(
	IMemoryPool *pmp
	)
	:
	CDXLPhysicalTableScan(pmp)
{}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalExternalScan::CDXLPhysicalExternalScan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalExternalScan::CDXLPhysicalExternalScan
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc
	)
	:
	CDXLPhysicalTableScan(pmp, pdxltabdesc)
{}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalExternalScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalExternalScan::Edxlop() const
{
	return EdxlopPhysicalExternalScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalExternalScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalExternalScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalExternalScan);
}

// EOF
