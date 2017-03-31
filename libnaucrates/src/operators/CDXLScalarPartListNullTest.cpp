//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal, Inc.
//
//  Implementation of DXL Part list null test expression

#include "naucrates/dxl/operators/CDXLScalarPartListNullTest.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

// Ctor
CDXLScalarPartListNullTest::CDXLScalarPartListNullTest
	(
	IMemoryPool *pmp,
	ULONG ulLevel,
	BOOL fIsNull
	)
	:
	CDXLScalar(pmp),
	m_ulLevel(ulLevel),
	m_fIsNull(fIsNull)
{
}

// Operator type
Edxlopid
CDXLScalarPartListNullTest::Edxlop() const
{
	return EdxlopScalarPartListNullTest;
}

// Operator name
const CWStringConst *
CDXLScalarPartListNullTest::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarPartListNullTest);
}

// Serialize operator in DXL format
void
CDXLScalarPartListNullTest::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode * // pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartLevel), m_ulLevel);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenScalarIsNull), m_fIsNull);
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

// partitioning level
ULONG
CDXLScalarPartListNullTest::UlLevel() const
{
	return m_ulLevel;
}

// Null Test type (true for 'is null', false for 'is not null')
BOOL
CDXLScalarPartListNullTest::FIsNull() const
{
	return m_fIsNull;
}

// does the operator return a boolean result
BOOL
CDXLScalarPartListNullTest::FBoolean
	(
	CMDAccessor * //pmda
	)
	const
{
	return true;
}

#ifdef GPOS_DEBUG
// Checks whether operator node is well-structured
void
CDXLScalarPartListNullTest::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL // fValidateChildren
	)
	const
{
	GPOS_ASSERT(0 == pdxln->UlArity());
}
#endif // GPOS_DEBUG

// conversion function
CDXLScalarPartListNullTest *
CDXLScalarPartListNullTest::PdxlopConvert
	(
	CDXLOperator *pdxlop
	)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(EdxlopScalarPartListNullTest == pdxlop->Edxlop());

	return dynamic_cast<CDXLScalarPartListNullTest*>(pdxlop);
}

// EOF
