//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalRowTrigger.cpp
//
//	@doc:
//		Implementation of DXL physical row trigger operator
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLPhysicalRowTrigger.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRowTrigger::CDXLPhysicalRowTrigger
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalRowTrigger::CDXLPhysicalRowTrigger
	(
	IMemoryPool *pmp,
	IMDId *pmdidRel,
	INT iType,
	DrgPul *pdrgpulOld,
	DrgPul *pdrgpulNew
	)
	:
	CDXLPhysical(pmp),
	m_pmdidRel(pmdidRel),
	m_iType(iType),
	m_pdrgpulOld(pdrgpulOld),
	m_pdrgpulNew(pdrgpulNew)
{
	GPOS_ASSERT(pmdidRel->FValid());
	GPOS_ASSERT(0 != iType);
	GPOS_ASSERT(NULL != pdrgpulNew || NULL != pdrgpulOld);
	GPOS_ASSERT_IMP(NULL != pdrgpulNew && NULL != pdrgpulOld,
			pdrgpulNew->UlLength() == pdrgpulOld->UlLength());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRowTrigger::~CDXLPhysicalRowTrigger
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalRowTrigger::~CDXLPhysicalRowTrigger()
{
	m_pmdidRel->Release();
	CRefCount::SafeRelease(m_pdrgpulOld);
	CRefCount::SafeRelease(m_pdrgpulNew);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRowTrigger::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalRowTrigger::Edxlop() const
{
	return EdxlopPhysicalRowTrigger;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRowTrigger::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalRowTrigger::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalRowTrigger);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRowTrigger::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRowTrigger::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidRel->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenRelationMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDType), m_iType);

	if (NULL != m_pdrgpulOld)
	{
		CWStringDynamic *pstrColsOld = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulOld);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOldCols), pstrColsOld);
		GPOS_DELETE(pstrColsOld);
	}

	if (NULL != m_pdrgpulNew)
	{
		CWStringDynamic *pstrColsNew = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulNew);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenNewCols), pstrColsNew);
		GPOS_DELETE(pstrColsNew);
	}

	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize project list
	(*pdxln)[0]->SerializeToDXL(pxmlser);

	// serialize physical child
	(*pdxln)[1]->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalRowTrigger::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalRowTrigger::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	GPOS_ASSERT(2 == pdxln->UlArity());
	CDXLNode *pdxlnChild = (*pdxln)[1];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}

#endif // GPOS_DEBUG


// EOF
