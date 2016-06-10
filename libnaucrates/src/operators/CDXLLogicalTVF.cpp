//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLLogicalTVF.cpp
//
//	@doc:
//		Implementation of DXL table-valued function
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLLogicalTVF.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::CDXLLogicalTVF
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLLogicalTVF::CDXLLogicalTVF
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	CMDName *pmdname,
	DrgPdxlcd *pdrgdxlcd
	)
	:CDXLLogical(pmp),
	m_pmdidFunc(pmdidFunc),
	m_pmdidRetType(pmdidRetType),
	m_pmdname(pmdname),
	m_pdrgdxlcd(pdrgdxlcd)
{
	GPOS_ASSERT(m_pmdidFunc->FValid());
	GPOS_ASSERT(m_pmdidRetType->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::~CDXLLogicalTVF
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLLogicalTVF::~CDXLLogicalTVF()
{
	m_pdrgdxlcd->Release();
	m_pmdidFunc->Release();
	m_pmdidRetType->Release();
	GPOS_DELETE(m_pmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalTVF::Edxlop() const
{
	return EdxlopLogicalTVF;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalTVF::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalTVF);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::UlArity
//
//	@doc:
//		Return number of return columns
//
//---------------------------------------------------------------------------
ULONG
CDXLLogicalTVF::UlArity() const
{
	return m_pdrgdxlcd->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::Pdxlcd
//
//	@doc:
//		Get the column descriptor at the given position
//
//---------------------------------------------------------------------------
const CDXLColDescr *
CDXLLogicalTVF::Pdxlcd
	(
	ULONG ul
	)
	const
{
	return (*m_pdrgdxlcd)[ul];
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::FDefinesColumn
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
BOOL
CDXLLogicalTVF::FDefinesColumn
	(
	ULONG ulColId
	)
	const
{
	const ULONG ulSize = UlArity();
	for (ULONG ulDescr = 0; ulDescr < ulSize; ulDescr++)
	{
		ULONG ulId = Pdxlcd(ulDescr)->UlID();
		if (ulId == ulColId)
		{
			return true;
		}
	}

	return false;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalTVF::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidFunc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenFuncId));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	m_pmdidRetType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	
	// serialize columns
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));
	GPOS_ASSERT(NULL != m_pdrgdxlcd);

	for (ULONG ul = 0; ul < UlArity(); ul++)
	{
		CDXLColDescr *pdxlcd = (*m_pdrgdxlcd)[ul];
		pdxlcd->SerializeToDXL(pxmlser);
	}

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenColumns));

	// serialize arguments
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalTVF::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalTVF::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) const
{
	// assert validity of function id and return type
	GPOS_ASSERT(NULL != m_pmdidFunc);
	GPOS_ASSERT(NULL != m_pmdidRetType);

	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnArg = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnArg->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnArg->Pdxlop()->AssertValid(pdxlnArg, fValidateChildren);
		}
	}
}

#endif // GPOS_DEBUG


// EOF
