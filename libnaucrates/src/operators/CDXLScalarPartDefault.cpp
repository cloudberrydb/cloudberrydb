//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartDefault.cpp
//
//	@doc:
//		Implementation of DXL Part Default expression
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarPartDefault.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartDefault::CDXLScalarPartDefault
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarPartDefault::CDXLScalarPartDefault
	(
	IMemoryPool *pmp,
	ULONG ulLevel
	)
	:
	CDXLScalar(pmp),
	m_ulLevel(ulLevel)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartDefault::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarPartDefault::Edxlop() const
{
	return EdxlopScalarPartDefault;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartDefault::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarPartDefault::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarPartDefault);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartDefault::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarPartDefault::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * // pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartLevel), m_ulLevel);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartDefault::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarPartDefault::AssertValid
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
