//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarArrayRef.cpp
//
//	@doc:
//		Implementation of DXL arrayrefs
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArrayRef.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::CDXLScalarArrayRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRef::CDXLScalarArrayRef
	(
	IMemoryPool *pmp,
	IMDId *pmdidElem,
	INT iTypeModifier,
	IMDId *pmdidArray,
	IMDId *pmdidReturn
	)
	:
	CDXLScalar(pmp),
	m_pmdidElem(pmdidElem),
	m_iTypeModifier(iTypeModifier),
	m_pmdidArray(pmdidArray),
	m_pmdidReturn(pmdidReturn)
{
	GPOS_ASSERT(m_pmdidElem->FValid());
	GPOS_ASSERT(m_pmdidArray->FValid());
	GPOS_ASSERT(m_pmdidReturn->FValid());
	GPOS_ASSERT(m_pmdidReturn->FEquals(m_pmdidElem) || m_pmdidReturn->FEquals(m_pmdidArray));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::~CDXLScalarArrayRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarArrayRef::~CDXLScalarArrayRef()
{
	m_pmdidElem->Release();
	m_pmdidArray->Release();
	m_pmdidReturn->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarArrayRef::Edxlop() const
{
	return EdxlopScalarArrayRef;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArrayRef::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarArrayRef);
}

INT
CDXLScalarArrayRef::ITypeModifier() const
{
	return m_iTypeModifier;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayRef::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidElem->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenArrayElementType));
	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), ITypeModifier());
	}
	m_pmdidArray->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenArrayType));
	m_pmdidReturn->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

	// serialize child nodes
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(3 == ulArity || 4 == ulArity);

	// first 2 children are index lists
	(*pdxln)[0]->SerializeToDXL(pxmlser);
	(*pdxln)[1]->SerializeToDXL(pxmlser);

	// 3rd child is the ref expression
	const CWStringConst *pstrRefExpr = CDXLTokens::PstrToken(EdxltokenScalarArrayRefExpr);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrRefExpr);
	(*pdxln)[2]->SerializeToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrRefExpr);

	// 4th child is the optional assign expression
	const CWStringConst *pstrAssignExpr = CDXLTokens::PstrToken(EdxltokenScalarArrayRefAssignExpr);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrAssignExpr);
	if (4 == ulArity)
	{
		(*pdxln)[3]->SerializeToDXL(pxmlser);
	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrAssignExpr);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::FBoolean
//
//	@doc:
//		Does the operator return boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarArrayRef::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidReturn)->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArrayRef::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarArrayRef::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnChild = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnChild->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
