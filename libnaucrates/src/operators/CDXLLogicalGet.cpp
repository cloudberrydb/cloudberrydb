//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLLogicalGet.cpp
//
//	@doc:
//		Implementation of DXL logical get operator
//		
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLLogicalGet.h"
#include "naucrates/dxl/operators/CDXLNode.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::CDXLLogicalGet
//
//	@doc:
//		Construct a logical get operator node given its table descriptor rtable entry
//
//---------------------------------------------------------------------------
CDXLLogicalGet::CDXLLogicalGet
	(
	IMemoryPool *pmp,
	CDXLTableDescr *pdxltabdesc
	)
	:CDXLLogical(pmp),
	 m_pdxltabdesc(pdxltabdesc)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::~CDXLLogicalGet
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CDXLLogicalGet::~CDXLLogicalGet()
{
	CRefCount::SafeRelease(m_pdxltabdesc);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLLogicalGet::Edxlop() const
{
	return EdxlopLogicalGet;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLLogicalGet::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenLogicalGet);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::Pdxltabdesc
//
//	@doc:
//		Table descriptor for the table scan
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CDXLLogicalGet::Pdxltabdesc() const
{
	return m_pdxltabdesc;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLLogicalGet::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *//pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);

	// serialize table descriptor
	m_pdxltabdesc->SerializeToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::FDefinesColumn
//
//	@doc:
//		Check if given column is defined by operator
//
//---------------------------------------------------------------------------
BOOL
CDXLLogicalGet::FDefinesColumn
	(
	ULONG ulColId
	)
	const
{
	const ULONG ulSize = m_pdxltabdesc->UlArity();
	for (ULONG ulDescr = 0; ulDescr < ulSize; ulDescr++)
	{
		ULONG ulId = m_pdxltabdesc->Pdxlcd(ulDescr)->UlID();
		if (ulId == ulColId)
		{
			return true;
		}
	}

	return false;
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLLogicalGet::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLLogicalGet::AssertValid
	(
	const CDXLNode *, //pdxln
	BOOL // fValidateChildren
	) const
{
	// assert validity of table descriptor
	GPOS_ASSERT(NULL != m_pdxltabdesc);
	GPOS_ASSERT(NULL != m_pdxltabdesc->Pmdname());
	GPOS_ASSERT(m_pdxltabdesc->Pmdname()->Pstr()->FValid());
}
#endif // GPOS_DEBUG

// EOF
