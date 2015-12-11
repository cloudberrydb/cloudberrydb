//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLScalarAggref.cpp
//
//	@doc:
//		Implementation of DXL AggRef
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarAggref.h"
#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/mdcache/CMDAccessor.h"

#include "naucrates/md/IMDAggregate.h"

using namespace gpopt;
using namespace gpmd;
using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::CDXLScalarAggref
//
//	@doc:
//		Constructs an AggRef node
//
//---------------------------------------------------------------------------
CDXLScalarAggref::CDXLScalarAggref
	(
	IMemoryPool *pmp,
	IMDId *pmdidAggOid,
	IMDId *pmdidResolvedRetType,
	BOOL fDistinct,
	EdxlAggrefStage edxlaggstage
	)
	:
	CDXLScalar(pmp),
	m_pmdidAgg(pmdidAggOid),
	m_pmdidResolvedRetType(pmdidResolvedRetType),
	m_fDistinct(fDistinct),
	m_edxlaggstage(edxlaggstage)
{
	GPOS_ASSERT(NULL != pmdidAggOid);
	GPOS_ASSERT_IMP(NULL != pmdidResolvedRetType, pmdidResolvedRetType->FValid());
	GPOS_ASSERT(m_pmdidAgg->FValid());
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::~CDXLScalarAggref
//
//	@doc:
//		dtor
//
//---------------------------------------------------------------------------
CDXLScalarAggref::~CDXLScalarAggref()
{
	m_pmdidAgg->Release();
	CRefCount::SafeRelease(m_pmdidResolvedRetType);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarAggref::Edxlop() const
{
	return EdxlopScalarAggref;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::Edxlaggstage
//
//	@doc:
//		AggRef AggStage
//
//---------------------------------------------------------------------------
EdxlAggrefStage
CDXLScalarAggref::Edxlaggstage() const
{
	return m_edxlaggstage;
}
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::PstrAggStage
//
//	@doc:
//		AggRef AggStage
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarAggref::PstrAggStage() const
{
	switch (m_edxlaggstage)
	{
		case EdxlaggstageNormal:
			return CDXLTokens::PstrToken(EdxltokenAggrefStageNormal);
		case EdxlaggstagePartial:
			return CDXLTokens::PstrToken(EdxltokenAggrefStagePartial);
		case EdxlaggstageIntermediate:
			return CDXLTokens::PstrToken(EdxltokenAggrefStageIntermediate);
		case EdxlaggstageFinal:
			return CDXLTokens::PstrToken(EdxltokenAggrefStageFinal);
		default:
			GPOS_ASSERT(!"Unrecognized aggregate stage");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarAggref::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarAggref);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::PmdidAgg
//
//	@doc:
//		Returns function id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarAggref::PmdidAgg() const
{
	return m_pmdidAgg;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::PmdidResolvedRetType
//
//	@doc:
//		Returns resolved type id
//
//---------------------------------------------------------------------------
IMDId *
CDXLScalarAggref::PmdidResolvedRetType() const
{
	return m_pmdidResolvedRetType;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::FDistinct
//
//	@doc:
//		TRUE if it's agg(DISTINCT ...)
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarAggref::FDistinct() const
{
	return m_fDistinct;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarAggref::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidAgg->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenAggrefOid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAggrefDistinct),m_fDistinct);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenAggrefStage), PstrAggStage());
	if (NULL != m_pmdidResolvedRetType)
	{
		m_pmdidResolvedRetType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	}
	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarAggref::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	const IMDAggregate *pmdagg = pmda->Pmdagg(m_pmdidAgg);
	return (IMDType::EtiBool == pmda->Pmdtype(pmdagg->PmdidTypeResult())->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarAggref::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarAggref::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	) 
	const
{
	EdxlAggrefStage edxlaggrefstage = ((CDXLScalarAggref*) pdxln->Pdxlop())->Edxlaggstage();

	GPOS_ASSERT((EdxlaggstageFinal >= edxlaggrefstage) && (EdxlaggstageNormal <= edxlaggrefstage));

	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnAggrefArg = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnAggrefArg->Pdxlop()->Edxloperatortype());
		
		if (fValidateChildren)
		{
			pdxlnAggrefArg->Pdxlop()->AssertValid(pdxlnAggrefArg, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
