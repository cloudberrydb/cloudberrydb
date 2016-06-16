//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLScalarBitmapBoolOp.cpp
//
//	@doc:
//		Implementation of DXL bitmap bool operator
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarBitmapBoolOp.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "naucrates/md/IMDType.h"
#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::CDXLScalarBitmapBoolOp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarBitmapBoolOp::CDXLScalarBitmapBoolOp
	(
	IMemoryPool *pmp,
	IMDId *pmdidType,
	EdxlBitmapBoolOp bitmapboolop
	)
	:
	CDXLScalar(pmp),
	m_pmdidType(pmdidType),
	m_bitmapboolop(bitmapboolop)
{
	GPOS_ASSERT(EdxlbitmapSentinel > bitmapboolop);
	GPOS_ASSERT(IMDId::FValid(pmdidType));
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::~CDXLScalarBitmapBoolOp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarBitmapBoolOp::~CDXLScalarBitmapBoolOp()
{
	m_pmdidType->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarBitmapBoolOp::Edxlop() const
{
	return EdxlopScalarBitmapBoolOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::PmdidType
//
//	@doc:
//		Return type
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarBitmapBoolOp::PmdidType() const
{
	return m_pmdidType;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::EdxlBitmapBoolOp
//
//	@doc:
//		Bitmap bool type
//
//---------------------------------------------------------------------------
CDXLScalarBitmapBoolOp::EdxlBitmapBoolOp
CDXLScalarBitmapBoolOp::Edxlbitmapboolop() const
{
	return m_bitmapboolop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::FBoolean
//
//	@doc:
//		Is operator returning a boolean value
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarBitmapBoolOp::FBoolean
	(
	CMDAccessor *pmda
	) 
	const
{
	return (IMDType::EtiBool == pmda->Pmdtype(m_pmdidType)->Eti());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarBitmapBoolOp::PstrOpName() const
{
	if (EdxlbitmapAnd == m_bitmapboolop)
	{
		return CDXLTokens::PstrToken(EdxltokenScalarBitmapAnd);
	}
	
	return CDXLTokens::PstrToken(EdxltokenScalarBitmapOr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarBitmapBoolOp::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	GPOS_CHECK_ABORT;

	const CWStringConst *pstrElemName = PstrOpName();

	GPOS_ASSERT(NULL != pstrElemName);
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	GPOS_CHECK_ABORT;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarBitmapBoolOp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarBitmapBoolOp::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	EdxlBitmapBoolOp edxlbitmapboolop = ((CDXLScalarBitmapBoolOp *) pdxln->Pdxlop())->Edxlbitmapboolop();

	GPOS_ASSERT( (edxlbitmapboolop == EdxlbitmapAnd) || (edxlbitmapboolop == EdxlbitmapOr));

	ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(2 == ulArity);
	

	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnArg = (*pdxln)[ul];
		Edxlopid edxlop = pdxlnArg->Pdxlop()->Edxlop();
		
		GPOS_ASSERT(EdxlopScalarBitmapBoolOp == edxlop || EdxlopScalarBitmapIndexProbe == edxlop);
		
		if (fValidateChildren)
		{
			pdxlnArg->Pdxlop()->AssertValid(pdxlnArg, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
