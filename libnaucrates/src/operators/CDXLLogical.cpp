//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogical.cpp
//
//	@doc:
//		Implementation of DXL logical operators
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLLogical.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogical::CDXLLogical
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLLogical::CDXLLogical
	(
	IMemoryPool *pmp
	)
	:
	CDXLOperator(pmp)
{
}

//---------------------------------------------------------------------------
//      @function:
//              CDXLLogical::Edxloperatortype
//
//      @doc:
//              Operator Type
//
//---------------------------------------------------------------------------
Edxloptype
CDXLLogical::Edxloperatortype() const
{
	return EdxloptypeLogical;
}




// EOF
