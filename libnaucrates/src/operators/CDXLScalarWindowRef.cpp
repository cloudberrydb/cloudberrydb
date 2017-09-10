//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLScalarWindowRef.cpp
//
//	@doc:
//		Implementation of DXL WindowRef
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLScalarWindowRef.h"
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
//		CDXLScalarWindowRef::CDXLScalarWindowRef
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CDXLScalarWindowRef::CDXLScalarWindowRef
	(
	IMemoryPool *pmp,
	IMDId *pmdidFunc,
	IMDId *pmdidRetType,
	BOOL fDistinct,
	BOOL fStarArg,
	BOOL fSimpleAgg,
	EdxlWinStage edxlwinstage,
	ULONG ulWinspecPosition
	)
	:
	CDXLScalar(pmp),
	m_pmdidFunc(pmdidFunc),
	m_pmdidRetType(pmdidRetType),
	m_fDistinct(fDistinct),
	m_fStarArg(fStarArg),
	m_fSimpleAgg(fSimpleAgg),
	m_edxlwinstage(edxlwinstage),
	m_ulWinspecPos(ulWinspecPosition)
{
	GPOS_ASSERT(m_pmdidFunc->FValid());
	GPOS_ASSERT(m_pmdidRetType->FValid());
	GPOS_ASSERT(EdxlwinstageSentinel != m_edxlwinstage);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::~CDXLScalarWindowRef
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CDXLScalarWindowRef::~CDXLScalarWindowRef()
{
	m_pmdidFunc->Release();
	m_pmdidRetType->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::Edxlop
//
//	@doc:
//		Operator type
//
//---------------------------------------------------------------------------
Edxlopid
CDXLScalarWindowRef::Edxlop() const
{
	return EdxlopScalarWindowRef;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::PstrWinStage
//
//	@doc:
//		Return window stage
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarWindowRef::PstrWinStage() const
{
	GPOS_ASSERT(EdxlwinstageSentinel > m_edxlwinstage);
	ULONG rgrgulMapping[][2] =
					{
					{EdxlwinstageImmediate, EdxltokenWindowrefStageImmediate},
					{EdxlwinstagePreliminary, EdxltokenWindowrefStagePreliminary},
					{EdxlwinstageRowKey, EdxltokenWindowrefStageRowKey}
					};

	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		if ((ULONG) m_edxlwinstage == pulElem[0])
		{
			Edxltoken edxltk = (Edxltoken) pulElem[1];
			return CDXLTokens::PstrToken(edxltk);
			break;
		}
	}

	GPOS_ASSERT(!"Unrecognized window stage");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::PstrOpName
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLScalarWindowRef::PstrOpName() const
{
	return CDXLTokens::PstrToken(EdxltokenScalarWindowref);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::SerializeToDXL
//
//	@doc:
//		Serialize operator in DXL format
//
//---------------------------------------------------------------------------
void
CDXLScalarWindowRef::SerializeToDXL
	(
	CXMLSerializer *pxmlser,
	const CDXLNode *pdxln
	)
	const
{
	const CWStringConst *pstrElemName = PstrOpName();

	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
	m_pmdidFunc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenWindowrefOid));
	m_pmdidRetType->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenTypeId));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowrefDistinct),m_fDistinct);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowrefStarArg),m_fStarArg);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowrefSimpleAgg),m_fSimpleAgg);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowrefStrategy), PstrWinStage());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenWindowrefWinSpecPos), m_ulWinspecPos);

	pdxln->SerializeChildrenToDXL(pxmlser);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), pstrElemName);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::FBoolean
//
//	@doc:
//		Does the operator return a boolean result
//
//---------------------------------------------------------------------------
BOOL
CDXLScalarWindowRef::FBoolean
	(
	CMDAccessor *pmda
	)
	const
{
	IMDId *pmdid = pmda->Pmdfunc(m_pmdidFunc)->PmdidTypeResult();
	return (IMDType::EtiBool == pmda->Pmdtype(pmdid)->Eti());
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CDXLScalarWindowRef::AssertValid
//
//	@doc:
//		Checks whether operator node is well-structured
//
//---------------------------------------------------------------------------
void
CDXLScalarWindowRef::AssertValid
	(
	const CDXLNode *pdxln,
	BOOL fValidateChildren
	)
	const
{
	EdxlWinStage edxlwinrefstage = ((CDXLScalarWindowRef*) pdxln->Pdxlop())->Edxlwinstage();

	GPOS_ASSERT((EdxlwinstageSentinel >= edxlwinrefstage));

	const ULONG ulArity = pdxln->UlArity();
	for (ULONG ul = 0; ul < ulArity; ++ul)
	{
		CDXLNode *pdxlnWinrefArg = (*pdxln)[ul];
		GPOS_ASSERT(EdxloptypeScalar == pdxlnWinrefArg->Pdxlop()->Edxloperatortype());

		if (fValidateChildren)
		{
			pdxlnWinrefArg->Pdxlop()->AssertValid(pdxlnWinrefArg, fValidateChildren);
		}
	}
}
#endif // GPOS_DEBUG

// EOF
