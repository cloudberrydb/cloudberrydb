//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartBoundOpen.cpp
//
//	@doc:
//		Implementation of DXL Part bound openness expression
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarPartBoundOpen.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundOpen::CDXLScalarPartBoundOpen
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarPartBoundOpen::CDXLScalarPartBoundOpen
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
//		CDXLScalarPartBoundOpen::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarPartBoundOpen::Edxlop() const
{
	return EdxlopScalarPartBoundOpen;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundOpen::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarPartBoundOpen::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarPartBoundOpen);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartBoundOpen::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarPartBoundOpen::SerializeToDXL
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
//		CDXLScalarPartBoundOpen::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarPartBoundOpen::AssertValid
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
