//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalIndexOnlyScan.cpp
//
//	@doc:
//		Implementation of DXL physical index only scan operators
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalIndexOnlyScan.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexOnlyScan::CDXLPhysicalIndexOnlyScan
//
//	@doc:
//		Construct an index only scan node given its table descriptor,
//		index descriptor and filter conditions on the index
//
//---------------------------------------------------------------------------
CDXLPhysicalIndexOnlyScan::CDXLPhysicalIndexOnlyScan
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc,
	CDXLIndexDescr *pdxlid,
	EdxlIndexScanDirection edxlisd
	)
	:
	CDXLPhysicalIndexScan(pmp, pdxltabdesc, pdxlid, edxlisd)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexOnlyScan::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalIndexOnlyScan::Edxlop() const
{
	return EdxlopPhysicalIndexOnlyScan;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalIndexOnlyScan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalIndexOnlyScan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalIndexOnlyScan);
}

// EOF
