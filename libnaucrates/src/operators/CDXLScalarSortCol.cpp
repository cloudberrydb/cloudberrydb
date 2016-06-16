//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSortCol.cpp
//
//	@doc:
//		Implementation of DXL sorting columns for sort and motion operator nodes
//---------------------------------------------------------------------------
#include "naucrates/dxl/operators/CDXLScalarSortCol.h"

#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::CDXLScalarSortCol
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLScalarSortCol::CDXLScalarSortCol
	(
	IMemoryPool *pmp,
	ULONG ulColId,
	IMDId *pmdidSortOp,
	CWStringConst *pstrSortOpName,
	BOOL fSortNullsFirst
	)
	:
	CDXLScalar(pmp),
	m_ulColId(ulColId),
	m_pmdidSortOp(pmdidSortOp),
	m_pstrSortOpName(pstrSortOpName),
	m_fSortNullsFirst(fSortNullsFirst)
{
	GPOS_ASSERT(m_pmdidSortOp->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::~CDXLScalarSortCol
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarSortCol::~CDXLScalarSortCol()
{
	m_pmdidSortOp->Release();
	GPOS_DELETE(m_pstrSortOpName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarSortCol::Edxlop() const
{
	return EdxlopScalarSortCol;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSortCol::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSortCol);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::UlColId
//
//	@doc:
//		Id of the sorting column
//
//---------------------------------------------------------------------------
ULONG
CDXLScalarSortCol::UlColId() const
{
	return m_ulColId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::PmdidSortOp
//
//	@doc:
//		Oid of the sorting operator for the column from the catalog
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarSortCol::PmdidSortOp() const
{
	return m_pmdidSortOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::FSortNullsFirst
//
//	@doc:
//		Whether nulls are sorted before other values
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarSortCol::FSortNullsFirst() const
{
	return m_fSortNullsFirst;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSortCol::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *// pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), m_ulColId);
	m_pmdidSortOp->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenSortOpId));	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSortOpName), m_pstrSortOpName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSortNullsFirst), m_fSortNullsFirst);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSortCol::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarSortCol::AssertValid
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
