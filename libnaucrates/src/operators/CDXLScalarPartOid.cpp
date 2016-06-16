//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarPartOid.cpp
//
//	@doc:
//		Implementation of DXL Part oid expression
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarPartOid.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartOid::CDXLScalarPartOid
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarPartOid::CDXLScalarPartOid
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
//		CDXLScalarPartOid::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarPartOid::Edxlop() const
{
	return EdxlopScalarPartOid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartOid::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarPartOid::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarPartOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarPartOid::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarPartOid::SerializeToDXL
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
//		CDXLScalarPartOid::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarPartOid::AssertValid
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
