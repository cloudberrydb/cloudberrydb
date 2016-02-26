//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLPhysicalDML.cpp
//
//	@doc:
//		Implementation of DXL physical DML operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"
#include "naucrates/dxl/operators/CDXLPhysicalDML.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLTableDescr.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::CDXLPhysicalDML
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLPhysicalDML::CDXLPhysicalDML
	(
	IMemoryPool *pmp,
	const EdxlDmlType edxldmltype,
	CDXLTableDescr *pdxltabdesc,
	DrgPul *pdrgpul,
	ULONG ulAction,
	ULONG ulOid,
	ULONG ulCtid,
	ULONG ulSegmentId,
	BOOL fPreserveOids,
	ULONG ulTupleOid,
	CDXLDirectDispatchInfo *pdxlddinfo,
	BOOL fInputSorted
	)
	:
	CDXLPhysical(pmp),
	m_edxldmltype(edxldmltype),
	m_pdxltabdesc(pdxltabdesc),
	m_pdrgpul(pdrgpul),
	m_ulAction(ulAction),
	m_ulOid(ulOid),
	m_ulCtid(ulCtid),
	m_ulSegmentId(ulSegmentId),
	m_fPreserveOids(fPreserveOids),
	m_ulTupleOid(ulTupleOid),
	m_pdxlddinfo(pdxlddinfo),
	m_fInputSorted(fInputSorted)
{
	GPOS_ASSERT(EdxldmlSentinel > edxldmltype);
	GPOS_ASSERT(NULL != pdxltabdesc);
	GPOS_ASSERT(NULL != pdrgpul);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::~CDXLPhysicalDML
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLPhysicalDML::~CDXLPhysicalDML()
{
	m_pdxltabdesc->Release();
	m_pdrgpul->Release();
	CRefCount::SafeRelease(m_pdxlddinfo);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLPhysicalDML::Edxlop() const
{
	return EdxlopPhysicalDML;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalDML::PstrOpName() const
{
	switch (m_edxldmltype)
	{
		case Edxldmlinsert:
				return CDXLTokens::PstrToken(EdxltokenPhysicalDMLInsert);
		case Edxldmldelete:
				return CDXLTokens::PstrToken(EdxltokenPhysicalDMLDelete);
		case Edxldmlupdate:
				return CDXLTokens::PstrToken(EdxltokenPhysicalDMLUpdate);
		default:
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::SerializeToDXL
//
//	@doc:
//		Serialize function descriptor in DXL format
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDML::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	CWStringDynamic *pstrCols = CDXLUtils::PstrSerialize(m_pmp, m_pdrgpul);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenColumns), pstrCols);
	GPOS_DELETE(pstrCols);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenActionColId), m_ulAction);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenOidColId), m_ulOid);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCtidColId), m_ulCtid);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGpSegmentIdColId), m_ulSegmentId);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenInputSorted), m_fInputSorted);
	
	if (Edxldmlupdate == m_edxldmltype)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenUpdatePreservesOids), m_fPreserveOids);
	}

	if (m_fPreserveOids)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTupleOidColId), m_ulTupleOid);
	}
	
	pdxln->SerializePropertiesToDXL(pxmlser);

	if (NULL != m_pdxlddinfo)
	{
		m_pdxlddinfo->Serialize(pxmlser);
	}
	else
	{
		// TODO:  - Oct 22, 2014; clean this code once the direct dispatch code for DML and SELECT is unified
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchInfo));
		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenDirectDispatchInfo));
	}
	
	// serialize project list
	(*pdxln)[0]->SerializeToDXL(pxmlser);

	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(pxmlser);
	
	// serialize physical child
	(*pdxln)[1]->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalDML::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLPhysicalDML::AssertValid
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
