//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC, Corp.
//
//	@filename:
//		CDXLScalarSubquery.cpp
//
//	@doc:
//		Implementation of subqueries computing scalar values
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLScalarSubquery.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubquery::CDXLScalarSubquery
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSubquery::CDXLScalarSubquery
	(
	IMemoryPool *pmp,
	ULONG ulColId
	)
	:
	CDXLScalar(pmp),
	m_ulColId(ulColId)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubquery::~CDXLScalarSubquery
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSubquery::~CDXLScalarSubquery()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubquery::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSubquery::Edxlop() const
{
	return EdxlopScalarSubquery;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubquery::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubquery::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSubquery);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubquery::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSubquery::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize computed column id
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), m_ulColId);

	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubquery::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarSubquery::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(1 == pdxln->UlArity());
	
	CDXLNode *pdxlnChild = (*pdxln)[0];
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());

	pdxln->AssertValid(fValidateChildren);
}
#endif // GPOS_DEBUG

// EOF
