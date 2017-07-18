//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CDXLScalarValuesList.cpp
//
//	@doc:
//		Implementation of DXL value list operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLScalarValuesList.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;


// constructs a value list node
CDXLScalarValuesList::CDXLScalarValuesList
	(
	IMemoryPool *pmp
	)
	:
	CDXLScalar(pmp)
{
}

// destructor
CDXLScalarValuesList::~CDXLScalarValuesList()
{
}

// operator type
Edxlopid
CDXLScalarValuesList::Edxlop() const
{
	return EdxlopScalarValuesList;
}

// operator name
const CWStringConst *
CDXLScalarValuesList::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarValuesList);
}

// serialize operator in DXL format
void
CDXLScalarValuesList::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	GPOS_CHECK_ABORT;

	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pdxln->SerializeChildrenToDXL(pxmlser);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	GPOS_CHECK_ABORT;
}

// conversion function
CDXLScalarValuesList *
CDXLScalarValuesList::PdxlopConvert
	(
	CDXLOperator *pdxlop
	)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(EdxlopScalarValuesList == pdxlop->Edxlop());

	return dynamic_cast<CDXLScalarValuesList*>(pdxlop);
}

// does the operator return a boolean result
BOOL
CDXLScalarValuesList::FBoolean
	(
	CMDAccessor * //pmda
	)
	const
{
	return false;
}

#ifdef GPOS_DEBUG

// checks whether operator node is well-structured
void
CDXLScalarValuesList::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	const ULONG ulArity = pdxln->UlArity();

	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnConstVal = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnConstVal->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnConstVal->Pdxlop()->AssertValid(pdxlnConstVal, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
