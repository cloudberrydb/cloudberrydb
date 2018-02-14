//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarCoerceToDomain.cpp
//
//	@doc:
//		Implementation of DXL scalar coerce
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarCoerceToDomain.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpopt;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarCoerceToDomain::CDXLScalarCoerceToDomain
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarCoerceToDomain::CDXLScalarCoerceToDomain
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
//		CDXLScalarCoerceToDomain::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarCoerceToDomain::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarCoerceToDomain);
}

// EOF
