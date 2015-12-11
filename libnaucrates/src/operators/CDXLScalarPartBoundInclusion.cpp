//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartBoundInclusion.cpp
//
//	@doc:
//		Implementation of DXL Part bound inclusion expression
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarPartBoundInclusion.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundInclusion::CDXLScalarPartBoundInclusion
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarPartBoundInclusion::CDXLScalarPartBoundInclusion
	(
	IMemoryPool *pmp,
	ULONG ulLevel,
	BOOL fLower
	)
	:
	CDXLScalar(pmp),
	m_ulLevel(ulLevel),
	m_fLower(fLower)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundInclusion::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarPartBoundInclusion::Edxlop() const
{
	return EdxlopScalarPartBoundInclusion;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundInclusion::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarPartBoundInclusion::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarPartBoundInclusion);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundInclusion::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarPartBoundInclusion::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * // pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartLevel), m_ulLevel);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenScalarPartBoundLower), m_fLower);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundInclusion::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarPartBoundInclusion::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	)
	const
{
	GPOS_ASSERT(0 == pdxln->UlArity());
}
#endif // GPOS_DEBUG

// EOF
