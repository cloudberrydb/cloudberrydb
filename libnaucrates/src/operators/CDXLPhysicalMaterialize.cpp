//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalMaterialize.cpp
//
//	@doc:
//		Implementation of DXL physical materialize operator
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalMaterialize.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::CDXLPhysicalMaterialize
//
//	@doc:
//		Construct a non-spooling materialize
//
//---------------------------------------------------------------------------
CDXLPhysicalMaterialize::CDXLPhysicalMaterialize
	(
	IMemoryPool *pmp,
	BOOL fEager
	)
	:
	CDXLPhysical(pmp),
	m_fEager(fEager),
	m_ulSpoolId(0),
	m_edxlsptype(EdxlspoolNone),
	m_iExecutorSlice(-1),
	m_ulConsumerSlices(0)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::CDXLPhysicalMaterialize
//
//	@doc:
//		Construct a spooling materialize
//
//---------------------------------------------------------------------------
CDXLPhysicalMaterialize::CDXLPhysicalMaterialize
	(
	IMemoryPool *pmp,
	BOOL fEager,
	ULONG ulSpoolId,
	INT iExecutorSlice,
	ULONG ulConsumerSlices
	)
	:
	CDXLPhysical(pmp),
	m_fEager(fEager),
	m_ulSpoolId(ulSpoolId),
	m_edxlsptype(EdxlspoolMaterialize),
	m_iExecutorSlice(iExecutorSlice),
	m_ulConsumerSlices(ulConsumerSlices)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalMaterialize::Edxlop() const
{
	return EdxlopPhysicalMaterialize;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalMaterialize::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenPhysicalMaterialize);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::FSpooling
//
//	@doc:
//		Is this a spooling materialize operator
//
//---------------------------------------------------------------------------
BOOL
CDXLPhysicalMaterialize::FSpooling() const
{
	return (EdxlspoolNone != m_edxlsptype);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::UlSpoolId
//
//	@doc:
//		Id of the spool if the materialize node is spooling
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalMaterialize::UlSpoolId() const
{
	return m_ulSpoolId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::IExecutorSlice
//
//	@doc:
//		Id of the slice executing the spool
//
//---------------------------------------------------------------------------
INT
CDXLPhysicalMaterialize::IExecutorSlice() const
{
	return m_iExecutorSlice;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::UlConsumerSlices
//
//	@doc:
//		Number of slices consuming the spool
//
//---------------------------------------------------------------------------
ULONG
CDXLPhysicalMaterialize::UlConsumerSlices() const
{
	return m_ulConsumerSlices;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::FEager
//
//	@doc:
//		Does the materialize node do eager materialization
//
//---------------------------------------------------------------------------
BOOL
CDXLPhysicalMaterialize::FEager() const
{
	return m_fEager;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMaterialize::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMaterializeEager), m_fEager);

	if (EdxlspoolMaterialize == m_edxlsptype)
	{
		// serialize spool info
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSpoolId), m_ulSpoolId);
		
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenExecutorSliceId), m_iExecutorSlice);
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenConsumerSliceCount), m_ulConsumerSlices);
	}
		
	// serialize properties
	pdxln->SerializePropertiesToDXL(pxmlser);

	// serialize children
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalMaterialize::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalMaterialize::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	GPOS_ASSERT(EdxlspoolNone == m_edxlsptype || EdxlspoolMaterialize == m_edxlsptype);
	GPOS_ASSERT(EdxlmatIndexSentinel == pdxln->UlArity());

	CDXLNode *pdxlnChild = (*pdxln)[EdxlmatIndexChild];
	GPOS_ASSERT(EdxloptypePhysical == pdxlnChild->Pdxlop()->Edxloperatortype());

	if (fValidateChildren)
	{
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG

// EOF
