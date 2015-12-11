//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarArray.cpp
//
//	@doc:
//		Implementation of DXL arrays
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarArray.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::CDXLScalarArray
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarArray::CDXLScalarArray
	(
	IMemoryPool *pmp,
	IMDId *pmdidElem,
	IMDId *pmdidArray,
	BOOL fMultiDimensional
	)
	:
	CDXLScalar(pmp),
	m_pmdidElem(pmdidElem),
	m_pmdidArray(pmdidArray),
	m_fMultiDimensional(fMultiDimensional)
{
	GPOS_ASSERT(m_pmdidElem->FValid());
	GPOS_ASSERT(m_pmdidArray->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::~CDXLScalarArray
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarArray::~CDXLScalarArray()
{
	m_pmdidElem->Release();
	m_pmdidArray->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarArray::Edxlop() const
{
	return EdxlopScalarArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarArray::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarArray);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::PmdidElem
//
//	@doc:
//		Id of base element type
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarArray::PmdidElem() const
{
	return m_pmdidElem;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::PmdidArray
//
//	@doc:
//		Id of array type
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarArray::PmdidArray() const
{
	return m_pmdidArray;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::FMultiDimensional
//
//	@doc:
//		Is this a multi-dimensional array
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarArray::FMultiDimensional() const
{
	return m_fMultiDimensional;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarArray::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidArray->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenArrayType));
	m_pmdidElem->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenArrayElementType));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenArrayMultiDim),m_fMultiDimensional);
	
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarArray::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarArray::AssertValid
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
