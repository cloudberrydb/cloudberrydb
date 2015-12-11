//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalProject.cpp
//
//	@doc:
//		Implementation of DXL logical project operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalProject.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::CDXLLogicalProject
//
//	@doc:
//		Construct a DXL Logical project node
//
//---------------------------------------------------------------------------
CDXLLogicalProject::CDXLLogicalProject
	(
	IMemoryPool *pmp
	)
	:CDXLLogical(pmp),
	 m_pmdnameAlias(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalProject::Edxlop() const
{
	return EdxlopLogicalProject;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::Pmdname
//
//	@doc:
//		Returns alias name
//
//---------------------------------------------------------------------------
const CMDName *
CDXLLogicalProject::Pmdname() const
{
	return m_pmdnameAlias;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::SetAliasName
//
//	@doc:
//		Set alias name
//
//---------------------------------------------------------------------------
void
CDXLLogicalProject::SetAliasName
	(
	CMDName *pmdname
	)
{
	GPOS_ASSERT(NULL == m_pmdnameAlias);
	GPOS_ASSERT(NULL != pmdname);

	m_pmdnameAlias = pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalProject::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalProject);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalProject::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize alias
	if (NULL != m_pmdnameAlias)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDerivedTableName), m_pmdnameAlias->Pstr());
	}

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalProject::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalProject::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	GPOS_ASSERT(2 == pdxln->UlArity());

	CDXLNode *pdxlnProjList = (*pdxln)[0];
	CDXLNode *pdxlnChild = (*pdxln)[1];

	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnProjList->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypeLogical == pdxlnChild->Pdxlop()->Edxloperatortype());
	
	if (fValidateChildren)
	{
		pdxlnProjList->Pdxlop()->AssertValid(pdxlnProjList, fValidateChildren);
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);		
	}

	const ULONG ulArity = pdxlnProjList->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnPrEl = (*pdxlnProjList)[ul];
		GPOS_ASSERT(EdxlopScalarIdent != pdxlnPrEl->Pdxlop()->Edxlop());
	}
}
#endif // GPOS_DEBUG

// EOF
