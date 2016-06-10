//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLNode.cpp
//
//	@doc:
//		Implementation of DXL nodes
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLDirectDispatchInfo.h"

#include "naucrates/dxl/operators/CDXLOperator.h"

using namespace gpos;
using namespace gpdxl;


//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Constructs a DXL node with unspecified operator
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp),
	m_pdxlop(NULL),
	m_pdxlprop(NULL),
	m_pdxlddinfo(NULL)
{
	m_pdrgpdxln = GPOS_NEW(pmp) DrgPdxln(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Constructs a DXL node with given operator
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode
	(
	IMemoryPool *pmp,
	CDXLOperator *pdxlop
	)
	:
	m_pmp(pmp),
	m_pdxlop(pdxlop),
	m_pdxlprop(NULL),
	m_pdxlddinfo(NULL)
{
	GPOS_ASSERT(NULL != pdxlop);
	m_pdrgpdxln = GPOS_NEW(pmp) DrgPdxln(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode
	(
	IMemoryPool *pmp,
	CDXLOperator *pdxlop,
	CDXLNode *pdxlnChild
	)
	:
	m_pmp(pmp),
	m_pdxlop(pdxlop),
	m_pdxlprop(NULL),
	m_pdrgpdxln(NULL),
	m_pdxlddinfo(NULL)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(NULL != pdxlnChild);

	m_pdrgpdxln = GPOS_NEW(pmp) DrgPdxln(pmp);
	m_pdrgpdxln->Append(pdxlnChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode
	(
	IMemoryPool *pmp,
	CDXLOperator *pdxlop,
	CDXLNode *pdxlnFst,
	CDXLNode *pdxlnSnd
	)
	:
	m_pmp(pmp),
	m_pdxlop(pdxlop),
	m_pdxlprop(NULL),
	m_pdrgpdxln(NULL),
	m_pdxlddinfo(NULL)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(NULL != pdxlnFst);
	GPOS_ASSERT(NULL != pdxlnSnd);
	
	m_pdrgpdxln = GPOS_NEW(pmp) DrgPdxln(pmp);
	m_pdrgpdxln->Append(pdxlnFst);
	m_pdrgpdxln->Append(pdxlnSnd);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode
	(
	IMemoryPool *pmp,
	CDXLOperator *pdxlop,
	CDXLNode *pdxlnFst,
	CDXLNode *pdxlnSnd,
	CDXLNode *pdxlnThrd
	)
	:
	m_pmp(pmp),
	m_pdxlop(pdxlop),
	m_pdxlprop(NULL),
	m_pdrgpdxln(NULL),
	m_pdxlddinfo(NULL)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(NULL != pdxlnFst);
	GPOS_ASSERT(NULL != pdxlnSnd);
	GPOS_ASSERT(NULL != pdxlnThrd);
	
	m_pdrgpdxln = GPOS_NEW(pmp) DrgPdxln(pmp);
	m_pdrgpdxln->Append(pdxlnFst);
	m_pdrgpdxln->Append(pdxlnSnd);
	m_pdrgpdxln->Append(pdxlnThrd);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::CDXLNode
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLNode::CDXLNode
	(
	IMemoryPool *pmp,
	CDXLOperator *pdxlop,
	DrgPdxln *pdrgpdxln
	)
	:
	m_pmp(pmp),
	m_pdxlop(pdxlop),
	m_pdxlprop(NULL),
	m_pdrgpdxln(pdrgpdxln),
	m_pdxlddinfo(NULL)
{
	GPOS_ASSERT(NULL != pdxlop);
	GPOS_ASSERT(NULL != pdrgpdxln);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::~CDXLNode
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLNode::~CDXLNode()
{
	m_pdrgpdxln->Release();
	CRefCount::SafeRelease(m_pdxlop);
	CRefCount::SafeRelease(m_pdxlprop);
	CRefCount::SafeRelease(m_pdxlddinfo);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::AddChild
//
//	@doc:
//		Adds a child to the DXL node's list of children
//
//---------------------------------------------------------------------------
void
CDXLNode::AddChild
	(
	CDXLNode *pdxlnChild
	)
{
	GPOS_ASSERT(NULL != m_pdrgpdxln);
	GPOS_ASSERT(NULL != pdxlnChild);

	m_pdrgpdxln->Append(pdxlnChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::ReplaceChild
//
//	@doc:
//		Replaces a child of the DXL node with a new one
//
//---------------------------------------------------------------------------
void
CDXLNode::ReplaceChild
	(
	ULONG ulPos,
	CDXLNode *pdxlnChild
	)
{
	GPOS_ASSERT(NULL != m_pdrgpdxln);
	GPOS_ASSERT(NULL != pdxlnChild);

	m_pdrgpdxln->Replace(ulPos, pdxlnChild);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SetOperator
//
//	@doc:
//		Sets the operator at that DXL node
//
//---------------------------------------------------------------------------
void
CDXLNode::SetOperator
	(
	CDXLOperator *pdxlop
	)
{
	GPOS_ASSERT(NULL == m_pdxlop);
	m_pdxlop = pdxlop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SerializeToDXL
//
//	@doc:
//		Serializes the node in DXL format
//
//---------------------------------------------------------------------------
void
CDXLNode::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	if (NULL != m_pdxlop)
	{
		m_pdxlop->SerializeToDXL(pxmlser, this);
	}	
	
	if (NULL != m_pdxlddinfo && 0 < m_pdxlddinfo->Pdrgpdrgpdxldatum()->UlLength())
	{
		m_pdxlddinfo->Serialize(pxmlser);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SerializeChildrenToDXL
//
//	@doc:
//		Serializes the node's children in DXL format
//
//---------------------------------------------------------------------------
void
CDXLNode::SerializeChildrenToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	// serialize children nodes
	const ULONG ulArity = UlArity();
	for (ULONG i = 0; i < ulArity; i++)
	{
		GPOS_CHECK_ABORT;

		CDXLNode *pdxlnChild = (*m_pdrgpdxln)[i];
		pdxlnChild->SerializeToDXL(pxmlser);

		GPOS_CHECK_ABORT;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SetProperties
//
//	@doc:
//		Set operator properties
//
//---------------------------------------------------------------------------
void
CDXLNode::SetProperties
	(
	CDXLProperties *pdxlprop
	)
{
	// allow setting properties only once
	GPOS_ASSERT(NULL == m_pdxlprop);
	m_pdxlprop = pdxlprop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SetDirectDispatchInfo
//
//	@doc:
//		Set direct dispatch info
//
//---------------------------------------------------------------------------
void
CDXLNode::SetDirectDispatchInfo
	(
	CDXLDirectDispatchInfo *pdxlddinfo
	)
{
	// allow setting direct dispatch info only once
	GPOS_ASSERT(NULL == m_pdxlddinfo);
	GPOS_ASSERT(NULL != pdxlddinfo);
	m_pdxlddinfo = pdxlddinfo;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::SerializePropertiesToDXL
//
//	@doc:
//		Serialize properties in DXL format
//
//---------------------------------------------------------------------------
void
CDXLNode::SerializePropertiesToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	m_pdxlprop->SerializePropertiesToDXL(pxmlser);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLNode::AssertValid
//
//	@doc:
//		Checks whether node is well-structured 
//
//---------------------------------------------------------------------------
void
CDXLNode::AssertValid
	(
	BOOL fValidateChildren
	) 
	const
{
	if (!fValidateChildren)
	{
		return;
	}
	
	const ULONG ulArity = UlArity();
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		CDXLNode *pdxlnChild = (*this)[ul];
		pdxlnChild->Pdxlop()->AssertValid(pdxlnChild, fValidateChildren);
	}
}
#endif // GPOS_DEBUG




// EOF
