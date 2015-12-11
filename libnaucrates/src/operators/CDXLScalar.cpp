//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalar.cpp
//
//	@doc:
//		Implementation of DXL scalar operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalar.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalar::CDXLScalar
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalar::CDXLScalar
	(
	IMemoryPool *pmp
	)
	:
	CDXLOperator(pmp)
{
}

//---------------------------------------------------------------------------
//      @function:
//              CDXLScalar::Edxloperatortype
//
//      @doc:
//              Operator Type
//
//---------------------------------------------------------------------------
Edxloptype
CDXLScalar::Edxloperatortype() const
{
	return EdxloptypeScalar;
}


// EOF
