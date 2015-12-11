//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLPhysicalCTAS.cpp
//
//	@doc:
//		Implementation of DXL physical CTAS operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLPhysicalCTAS.h"
#include "naucrates/dxl/operators/CDXLCtasStorageOptions.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTAS::CDXLPhysicalCTAS
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalCTAS::CDXLPhysicalCTAS
	(
	IMemoryPool *pmp,
	CMDName *pmdnameSchema,
	CMDName *pmdnameRel,
	DrgPdxlcd *pdrgpdxlcd,
	CDXLCtasStorageOptions *pdxlctasopt,
	IMDRelation::Ereldistrpolicy ereldistrpolicy,
	DrgPul *pdrgpulDistr,
	BOOL fTemporary,
	BOOL fHasOids, 
	IMDRelation::Erelstoragetype erelstorage,
	DrgPul *pdrgpulSource,
	DrgPi *pdrgpiVarTypeMod
	)
	:
	CDXLPhysical(pmp), 
	m_pmdnameSchema(pmdnameSchema),
	m_pmdnameRel(pmdnameRel),
	m_pdrgpdxlcd(pdrgpdxlcd),
	m_pdxlctasopt(pdxlctasopt),
	m_ereldistrpolicy(ereldistrpolicy),
	m_pdrgpulDistr(pdrgpulDistr),
	m_fTemporary(fTemporary),
	m_fHasOids(fHasOids),
	m_erelstorage(erelstorage),
	m_pdrgpulSource(pdrgpulSource),
	m_pdrgpiVarTypeMod(pdrgpiVarTypeMod)
{
	GPOS_ASSERT(NULL != pmdnameRel);
	GPOS_ASSERT(NULL != pdrgpdxlcd);
	GPOS_ASSERT(NULL != pdxlctasopt);
	GPOS_ASSERT_IFF(IMDRelation::EreldistrHash == ereldistrpolicy, NULL != pdrgpulDistr);
	GPOS_ASSERT(NULL != pdrgpulSource);
	GPOS_ASSERT(NULL != pdrgpiVarTypeMod);
	GPOS_ASSERT(pdrgpdxlcd->UlLength() == pdrgpiVarTypeMod->UlLength());
	GPOS_ASSERT(IMDRelation::ErelstorageSentinel > erelstorage);
	GPOS_ASSERT(IMDRelation::EreldistrSentinel > ereldistrpolicy);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTAS::~CDXLPhysicalCTAS
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalCTAS::~CDXLPhysicalCTAS()
{
	GPOS_DELETE(m_pmdnameSchema);
	GPOS_DELETE(m_pmdnameRel);
	m_pdrgpdxlcd->Release();
	m_pdxlctasopt->Release();
	CRefCount::SafeRelease(m_pdrgpulDistr);
	m_pdrgpulSource->Release();
	m_pdrgpiVarTypeMod->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTAS::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalCTAS::Edxlop() const
{
	return EdxlopPhysicalCTAS;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTAS::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalCTAS::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalCTAS);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTAS::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalCTAS::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	if (NULL != m_pmdnameSchema)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSchema), m_pmdnameSchema->Pstr());
	}
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdnameRel->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelTemporary), m_fTemporary);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelHasOids), m_fHasOids);
	GPOS_ASSERT(NULL != IMDRelation::PstrStorageType(m_erelstorage));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelStorageType), IMDRelation::PstrStorageType(m_erelstorage));

	// serialize distribution columns
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenRelDistrPolicy), IMDRelation::PstrDistrPolicy(m_ereldistrpolicy));
	
	if (IMDRelation::EreldistrHash == m_ereldistrpolicy)
	{
		GPOS_ASSERT(NULL != m_pdrgpulDistr);
		
		// serialize distribution columns
		CWStringDynamic *pstrDistrColumns = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulDistr);
		GPOS_ASSERT(NULL != pstrDistrColumns);
		
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDistrColumns), pstrDistrColumns);
		GPOS_DELETE(pstrDistrColumns);
	}

	// serialize input columns
	CWStringDynamic *pstrCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulSource);
	GPOS_ASSERT(NULL != pstrCols);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInsertCols), pstrCols);
	GPOS_DELETE(pstrCols);
	
	// serialize vartypmod list
	CWStringDynamic *pstrVarTypeModList = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpiVarTypeMod);
	GPOS_ASSERT(NULL != pstrVarTypeModList);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenVarTypeModList), pstrVarTypeModList);
	GPOS_DELETE(pstrVarTypeModList);

	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);
	
	// serialize column descriptors
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));
	
	const ULONG ulArity = m_pdrgpdxlcd->UlLength();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLColDescr *pdxlcd = (*m_pdrgpdxlcd)[ul];
		pdxlcd->SerializeToDXL(pxmlser);
	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));

	m_pdxlctasopt->Serialize(pxmlser);
	
	// serialize arguments
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalCTAS::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalCTAS::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(2 == pdxln->UlArity());

	CDXLNode *pdxlnPrL = (*pdxln)[0];
	CDXLNode *pdxlnChild = (*pdxln)[1];
	GPOS_ASSERT(EdxlopScalarProjectList == pdxlnPrL->Pdxlop()->Edxlop());
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
