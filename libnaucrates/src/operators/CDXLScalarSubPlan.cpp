//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarSubPlan.cpp
//
//	@doc:
//		Implementation of DXL Scalar SubPlan operator
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "gpopt/base/COptCtxt.h"

#include "naucrates/dxl/operators/CDXLScalarSubPlan.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;
using namespace gpopt;
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::CDXLScalarSubPlan
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarSubPlan::CDXLScalarSubPlan
	(
	IMemoryPool *pmp,
	IMDId *pmdidFirstColType,
	DrgPdxlcr *pdrgdxlcr,
	EdxlSubPlanType edxlsubplantype,
	CDXLNode *pdxlnTestExpr
	)
	:
	CDXLScalar(pmp),
	m_pmdidFirstColType(pmdidFirstColType),
	m_pdrgdxlcr(pdrgdxlcr),
	m_edxlsubplantype(edxlsubplantype),
	m_pdxlnTestExpr(pdxlnTestExpr)
{
	GPOS_ASSERT(EdxlSubPlanTypeSentinel > edxlsubplantype);
	GPOS_ASSERT_IMP(EdxlSubPlanTypeAny == edxlsubplantype || EdxlSubPlanTypeAll == edxlsubplantype, NULL != pdxlnTestExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::~CDXLScalarSubPlan
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarSubPlan::~CDXLScalarSubPlan()
{
	m_pmdidFirstColType->Release();
	m_pdrgdxlcr->Release();
	CRefCount::SafeRelease(m_pdxlnTestExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubPlan::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarSubPlan);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::PmdidFirstColType
//
//	@doc:
//		Return type id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarSubPlan::PmdidFirstColType() const
{
	return m_pmdidFirstColType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarSubPlan::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidFirstColType)->Eti());
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::PstrSubplanType
//
//	@doc:
//		Return a string representation of Subplan type
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarSubPlan::PstrSubplanType() const
{
	switch (m_edxlsubplantype)
	{
		case EdxlSubPlanTypeScalar:
			return CDXLTokens::PstrToken(EdxltokenScalarSubPlanTypeScalar);

		case EdxlSubPlanTypeExists:
			return CDXLTokens::PstrToken(EdxltokenScalarSubPlanTypeExists);

		case EdxlSubPlanTypeNotExists:
			return CDXLTokens::PstrToken(EdxltokenScalarSubPlanTypeNotExists);

		case EdxlSubPlanTypeAny:
			return CDXLTokens::PstrToken(EdxltokenScalarSubPlanTypeAny);

		case EdxlSubPlanTypeAll:
			return CDXLTokens::PstrToken(EdxltokenScalarSubPlanTypeAll);

		default:
			GPOS_ASSERT(!"Unrecognized subplan type");
			return NULL;
	}
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarSubPlan::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidFirstColType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenScalarSubPlanType), PstrSubplanType());

	// serialize test expression
	pxmlser->OpenElement
					(
					CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
					CDXLTokens::PstrToken(EdxltokenScalarSubPlanTestExpr)
					);

	if (NULL != m_pdxlnTestExpr)
	{
		m_pdxlnTestExpr->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement
					(
					CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
					CDXLTokens::PstrToken(EdxltokenScalarSubPlanTestExpr)
					);

	pxmlser->OpenElement
				(
				CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
				CDXLTokens::PstrToken(EdxltokenScalarSubPlanParamList)
				);

	for (ULONG ul = 0; ul < m_pdrgdxlcr->UlLength(); ul++)
	{
		pxmlser->OpenElement
					(
					CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
					CDXLTokens::PstrToken(EdxltokenScalarSubPlanParam)
					);

		ULONG ulid = (*m_pdrgdxlcr)[ul]->UlID();
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColId), ulid);

		const CMDName *pmdname = (*m_pdrgdxlcr)[ul]->Pmdname();
		const IMDId *pmdidType = (*m_pdrgdxlcr)[ul]->PmdidType();
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColName), pmdname->Pstr());
		pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));

		pxmlser->CloseElement
					(
					CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
					CDXLTokens::PstrToken(EdxltokenScalarSubPlanParam)
					);
	}

	pxmlser->CloseElement
				(
				CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
				CDXLTokens::PstrToken(EdxltokenScalarSubPlanParamList)
				);

	GPOS_ASSERT(1 == pdxln->PdrgpdxlnChildren()->UlLength());

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarSubPlan::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarSubPlan::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(EdxlSubPlanIndexSentinel == pdxln->UlArity());

	// assert child plan is a physical plan and is valid

	CDXLNode *pdxlnChild = (*pdxln)[EdxlSubPlanIndexChildPlan];
	GPOS_ASSERT(NULL != pdxlnChild);
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT_IMP(NULL != m_pdxlnTestExpr, EdxloptypeScalar == m_pdxlnTestExpr->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
