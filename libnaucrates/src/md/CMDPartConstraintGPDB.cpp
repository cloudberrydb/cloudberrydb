//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDPartConstraintGPDB.cpp
//
//	@doc:
//		Implementation of part constraints in the MD cache
//---------------------------------------------------------------------------

#include "naucrates/md/CMDPartConstraintGPDB.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/translate/CTranslatorDXLToExpr.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CMDPartConstraintGPDB::CMDPartConstraintGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDPartConstraintGPDB::CMDPartConstraintGPDB
	(
	IMemoryPool *pmp,
	DrgPul *pdrgpulDefaultParts,
	BOOL fUnbounded,
	CDXLNode *pdxln
	)
	:
	m_pmp(pmp),
	m_pdrgpulDefaultParts(pdrgpulDefaultParts),
	m_fUnbounded(fUnbounded),
	m_pdxln(pdxln)
{
	GPOS_ASSERT(NULL != pdrgpulDefaultParts);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDPartConstraintGPDB::~CMDPartConstraintGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDPartConstraintGPDB::~CMDPartConstraintGPDB()
{
	if (NULL != m_pdxln)
		m_pdxln->Release();
	m_pdrgpulDefaultParts->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDPartConstraintGPDB::Pexpr
//
//	@doc:
//		Scalar expression of the check constraint
//
//---------------------------------------------------------------------------
CExpression *
CMDPartConstraintGPDB::Pexpr
	(
	IMemoryPool *pmp,
	CMDAccessor *pmda,
	DrgPcr *pdrgpcr
	)
	const
{
	GPOS_ASSERT(NULL != pdrgpcr);

	// translate the DXL representation of the part constraint expression
	CTranslatorDXLToExpr dxltr(pmp, pmda);
	return dxltr.PexprTranslateScalar(m_pdxln, pdrgpcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDPartConstraintGPDB::PdrgpulDefaultParts
//
//	@doc:
//		Included default partitions
//
//---------------------------------------------------------------------------
DrgPul *
CMDPartConstraintGPDB::PdrgpulDefaultParts() const
{
	return m_pdrgpulDefaultParts;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDPartConstraintGPDB::FUnbounded
//
//	@doc:
//		Is constraint unbounded
//
//---------------------------------------------------------------------------
BOOL
CMDPartConstraintGPDB::FUnbounded() const
{
	return m_fUnbounded;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDPartConstraintGPDB::Serialize
//
//	@doc:
//		Serialize part constraint in DXL format
//
//---------------------------------------------------------------------------
void
CMDPartConstraintGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenPartConstraint));
	
	// serialize default parts
	CWStringDynamic *pstrDefParts = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpulDefaultParts);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenDefaultPartition), pstrDefParts);
	GPOS_DELETE(pstrDefParts);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenPartConstraintUnbounded), m_fUnbounded);

	// serialize the scalar expression
	if (NULL != m_pdxln)
		m_pdxln->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenPartConstraint));

	GPOS_CHECK_ABORT;
}

// EOF
