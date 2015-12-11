//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarDistinctComp.cpp
//
//	@doc:
//		Implementation of DXL "is distinct from" comparison operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLScalarDistinctComp.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDistinctComp::CDXLScalarDistinctComp
//
//	@doc:
//		Constructs a scalar distinct comparison node
//
//---------------------------------------------------------------------------
CDXLScalarDistinctComp::CDXLScalarDistinctComp
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp
	)
	:
	CDXLScalarComp(pmp, pmdidOp, GPOS_NEW(pmp) CWStringConst(pmp, CDXLTokens::PstrToken(EdxltokenEq)->Wsz()))
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDistinctComp::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarDistinctComp::Edxlop() const
{
	return EdxlopScalarDistinct;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDistinctComp::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarDistinctComp::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarDistinctComp);;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDistinctComp::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarDistinctComp::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenOpNo));
	
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);	
}



#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarDistinctComp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLScalarDistinctComp::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{	
	GPOS_ASSERT(EdxlscdistcmpSentinel == pdxln->UlArity());
	
	CDXLNode *pdxlnLeft = (*pdxln)[EdxlscdistcmpIndexLeft];
	CDXLNode *pdxlnRight = (*pdxln)[EdxlscdistcmpIndexRight];
	
	// assert children are of right type (scalar)
	GPOS_ASSERT(EdxloptypeScalar == pdxlnLeft->Pdxlop()->Edxloperatortype());
	GPOS_ASSERT(EdxloptypeScalar == pdxlnRight->Pdxlop()->Edxloperatortype());
	
	GPOS_ASSERT(PstrCmpOpName()->FEquals(CDXLTokens::PstrToken(EdxltokenEq)));
	
	if (fValidateChildren)
	{
		pdxlnLeft->Pdxlop()->AssertValid(pdxlnLeft, fValidateChildren);
		pdxlnRight->Pdxlop()->AssertValid(pdxlnRight, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
