//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarComp.cpp
//
//	@doc:
//		Implementation of DXL comparison operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLScalarComp.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::CDXLScalarComp
//
//	@doc:
//		Constructs a scalar comparison node
//
//---------------------------------------------------------------------------
CDXLScalarComp::CDXLScalarComp
	(
	IMemoryPool *pmp,
	IMDId *pmdidOp,
	const CWStringConst *pstrCompOpName
	)
	:
	CDXLScalar(pmp),
	m_pmdid(pmdidOp),
	m_pstrCompOpName(pstrCompOpName)
{
	GPOS_ASSERT(m_pmdid->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::~CDXLScalarComp
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLScalarComp::~CDXLScalarComp()
{
	m_pmdid->Release();
	GPOS_DELETE(m_pstrCompOpName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::PstrCmpOpName
//
//	@doc:
//		Comparison operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarComp::PstrCmpOpName() const
{
	return m_pstrCompOpName;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::Pmdid
//
//	@doc:
//		Comparison operator id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarComp::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarComp::Edxlop() const
{
	return EdxlopScalarCmp;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarComp::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarComp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarComp::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	GPOS_CHECK_ABORT;

	const CWStringConst *pstrElemName = PstrOpName();
	
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenComparisonOp), PstrCmpOpName());

	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenOpNo));
	
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	GPOS_CHECK_ABORT;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarComp::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarComp::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	const ULONG ulArity = pdxln->UlArity();
	GPOS_ASSERT(2 == ulArity);

	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnArg = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnArg->Pdxlop()->Edxloperatortype() ||
					EdxloptypeLogical == pdxlnArg->Pdxlop()->Edxloperatortype());
		
		if (fValidateChildren)
		{
			pdxlnArg->Pdxlop()->AssertValid(pdxlnArg, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG





// EOF
