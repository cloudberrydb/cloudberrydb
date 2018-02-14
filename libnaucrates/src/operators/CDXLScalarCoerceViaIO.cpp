//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarCoerceViaIO.cpp
//
//	@doc:
//		Implementation of DXL scalar coerce
//
//	@owner:
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCoerceViaIO.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceViaIO::CDXLScalarCoerceViaIO
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCoerceViaIO::CDXLScalarCoerceViaIO
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	INT iTypeModifier,
	EdxlCoercionForm edxlcf,
	INT iLoc
	)
	:
	CDXLScalarCoerceBase(pmp, pmdidType, iTypeModifier, edxlcf, iLoc)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceViaIO::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarCoerceViaIO::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarCoerceViaIO);
}

// EOF
