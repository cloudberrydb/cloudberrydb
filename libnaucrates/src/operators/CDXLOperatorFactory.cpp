//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLOperatorFactory.cpp
//
//	@doc:
//		Implementation of the factory methods for creation of DXL elements.
//---------------------------------------------------------------------------

#include "gpos/string/CWStringConst.h"
#include "gpos/string/CWStringDynamic.h"
#include "gpos/common/clibwrapper.h"

#include "naucrates/md/CMDIdColStats.h"
#include "naucrates/md/CMDIdRelStats.h"
#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/CMDIdGPDBCtas.h"
#include "naucrates/md/CMDIdCast.h"
#include "naucrates/md/CMDIdScCmp.h"

#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"
#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"
#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/operators/CDXLDatumBool.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"

#include <xercesc/util/NumberFormatException.hpp>

using namespace gpos;
using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

#define GPDXL_GPDB_MDID_COMPONENTS 3
#define GPDXL_DEFAULT_USERID 0

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopTblScan
//
//	@doc:
//		Construct a table scan operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopTblScan
	(
	CDXLMemoryManager *pmm,
	const Attributes & // attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	return GPOS_NEW(pmp) CDXLPhysicalTableScan(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopSubqScan
//
//	@doc:
//		Construct a subquery scan operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopSubqScan
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse subquery name from attributes
	const XMLCh *xmlszAlias = XmlstrFromAttrs
								(
								attrs,
								EdxltokenAlias,
								EdxltokenPhysicalSubqueryScan
								);

	CWStringDynamic *pstrAlias = CDXLUtils::PstrFromXMLCh(pmm,xmlszAlias);
	

	// create a copy of the string in the CMDName constructor
	CMDName *pmdnameAlias = GPOS_NEW(pmp) CMDName(pmp, pstrAlias);
	
	GPOS_DELETE(pstrAlias);

	return GPOS_NEW(pmp) CDXLPhysicalSubqueryScan(pmp, pmdnameAlias);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopResult
//
//	@doc:
//		Construct a result operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopResult
	(
	CDXLMemoryManager *pmm
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLPhysicalResult(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopHashJoin
//
//	@doc:
//		Construct a hashjoin operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopHashJoin
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	const XMLCh *xmlszJoinType = XmlstrFromAttrs
									(
									attrs,
									EdxltokenJoinType,
									EdxltokenPhysicalHashJoin
									);
	
	EdxlJoinType edxljt = EdxljtParseJoinType(xmlszJoinType, CDXLTokens::PstrToken(EdxltokenPhysicalHashJoin));
	
	return GPOS_NEW(pmp) CDXLPhysicalHashJoin(pmp, edxljt);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopNLJoin
//
//	@doc:
//		Construct a nested loop join operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopNLJoin
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	const XMLCh *xmlszJoinType = XmlstrFromAttrs
									(
									attrs,
									EdxltokenJoinType,
									EdxltokenPhysicalNLJoin
									);
	
	BOOL fIndexNLJ = false;
	const XMLCh *xmlszIndexNLJ = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenPhysicalNLJoinIndex));
	if (NULL != xmlszIndexNLJ)
	{
		fIndexNLJ = FValueFromXmlstr
						(
						pmm,
						xmlszIndexNLJ,
						EdxltokenPhysicalNLJoinIndex,
						EdxltokenPhysicalNLJoin
						);
	}

	EdxlJoinType edxljt = EdxljtParseJoinType(xmlszJoinType, CDXLTokens::PstrToken(EdxltokenPhysicalNLJoin));
	
	return GPOS_NEW(pmp) CDXLPhysicalNLJoin(pmp, edxljt, fIndexNLJ);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopMergeJoin
//
//	@doc:
//		Construct a merge join operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopMergeJoin
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	const XMLCh *xmlszJoinType = XmlstrFromAttrs
									(
									attrs,
									EdxltokenJoinType,
									EdxltokenPhysicalMergeJoin
									);
	
	EdxlJoinType edxljt = EdxljtParseJoinType(xmlszJoinType, CDXLTokens::PstrToken(EdxltokenPhysicalMergeJoin));
	
	BOOL fUniqueOuter = FValueFromAttrs
								(
								pmm,
								attrs,
								EdxltokenMergeJoinUniqueOuter,
								EdxltokenPhysicalMergeJoin
								);
	
	return GPOS_NEW(pmp) CDXLPhysicalMergeJoin(pmp, edxljt, fUniqueOuter);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopGatherMotion
//
//	@doc:
//		Construct a gather motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopGatherMotion
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{	
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	CDXLPhysicalGatherMotion *pdxlop = GPOS_NEW(pmp) CDXLPhysicalGatherMotion(pmp);
	SetSegmentInfo(pmm, pdxlop, attrs, EdxltokenPhysicalGatherMotion);

	return pdxlop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopBroadcastMotion
//
//	@doc:
//		Construct a broadcast motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopBroadcastMotion
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	CDXLPhysicalBroadcastMotion *pdxlop = GPOS_NEW(pmp) CDXLPhysicalBroadcastMotion(pmp);
	SetSegmentInfo(pmm, pdxlop, attrs, EdxltokenPhysicalBroadcastMotion);
	
	return pdxlop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopRedistributeMotion
//
//	@doc:
//		Construct a redistribute motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopRedistributeMotion
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	BOOL fDuplicateSensitive = false;
	
	const XMLCh *xmlszDuplicateSensitive = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenDuplicateSensitive));
	if (NULL != xmlszDuplicateSensitive)
	{
		fDuplicateSensitive = FValueFromXmlstr(pmm, xmlszDuplicateSensitive, EdxltokenDuplicateSensitive, EdxltokenPhysicalRedistributeMotion);
	}
		
	CDXLPhysicalRedistributeMotion *pdxlop = GPOS_NEW(pmp) CDXLPhysicalRedistributeMotion(pmp, fDuplicateSensitive);
	SetSegmentInfo(pmm, pdxlop, attrs, EdxltokenPhysicalRedistributeMotion);
	
	
	
	return pdxlop;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopRoutedMotion
//
//	@doc:
//		Construct a routed motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopRoutedMotion
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	ULONG ulSegmentIdCol = CDXLOperatorFactory::UlValueFromAttrs(pmm, attrs, EdxltokenSegmentIdCol, EdxltokenPhysicalRoutedDistributeMotion);
	
	CDXLPhysicalRoutedDistributeMotion *pdxlop = GPOS_NEW(pmp) CDXLPhysicalRoutedDistributeMotion(pmp, ulSegmentIdCol);
	SetSegmentInfo(pmm, pdxlop, attrs, EdxltokenPhysicalRoutedDistributeMotion);
	
	return pdxlop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopRandomMotion
//
//	@doc:
//		Construct a random motion operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopRandomMotion
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	BOOL fDuplicateSensitive = false;

	const XMLCh *xmlszDuplicateSensitive = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenDuplicateSensitive));
	if (NULL != xmlszDuplicateSensitive)
	{
		fDuplicateSensitive = FValueFromXmlstr(pmm, xmlszDuplicateSensitive, EdxltokenDuplicateSensitive, EdxltokenPhysicalRandomMotion);
	}

	CDXLPhysicalRandomMotion *pdxlop = GPOS_NEW(pmp) CDXLPhysicalRandomMotion(pmp, fDuplicateSensitive);
	SetSegmentInfo(pmm, pdxlop, attrs, EdxltokenPhysicalRandomMotion);
	
	return pdxlop;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopAppend
//	@doc:
//		Construct an Append operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopAppend
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	BOOL fIsTarget = FValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenAppendIsTarget,
						EdxltokenPhysicalAppend
						);
	
	BOOL fIsZapped = FValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenAppendIsZapped,
						EdxltokenPhysicalAppend
						);

	return GPOS_NEW(pmp) CDXLPhysicalAppend(pmp, fIsTarget, fIsZapped);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopLimit
//	@doc:
//		Construct a Limit operator
//
//---------------------------------------------------------------------------
CDXLPhysical*
CDXLOperatorFactory::PdxlopLimit
	(
	CDXLMemoryManager *pmm,
	const Attributes & // attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLPhysicalLimit(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopLimitCount
//
//	@doc:
//		Construct a Limit Count operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopLimitCount
	(
	CDXLMemoryManager *pmm,
	const Attributes & // attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLScalarLimitCount(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopLimitOffset
//
//	@doc:
//		Construct a Limit Offset operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopLimitOffset
	(
	CDXLMemoryManager *pmm,
	const Attributes & // attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLScalarLimitOffset(pmp);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopAgg
//
//	@doc:
//		Construct an aggregate operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopAgg
	(
	CDXLMemoryManager *pmm,
	const Attributes & attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	const XMLCh *xmlszAggStrategy = XmlstrFromAttrs
										(
										attrs,
										EdxltokenAggStrategy,
										EdxltokenPhysicalAggregate
										);
	
	EdxlAggStrategy edxlaggstr = EdxlaggstrategySentinel;
	
	if (0 == XMLString::compareString
							(
							CDXLTokens::XmlstrToken(EdxltokenAggStrategyPlain),
							xmlszAggStrategy
							))
	{
		edxlaggstr = EdxlaggstrategyPlain;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggStrategySorted),
									xmlszAggStrategy))
	{
		edxlaggstr = EdxlaggstrategySorted;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggStrategyHashed),
									xmlszAggStrategy))
	{
		edxlaggstr = EdxlaggstrategyHashed;
	}
	else
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenAggStrategy)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenPhysicalAggregate)->Wsz()
			);		
	}
	
	BOOL fStreamSafe = false;

	const XMLCh *xmlszStreamSafe = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenAggStreamSafe));
	if (NULL != xmlszStreamSafe)
	{
		fStreamSafe = FValueFromXmlstr
						(
						pmm,
						xmlszStreamSafe,
						EdxltokenAggStreamSafe,
						EdxltokenPhysicalAggregate
						);
	}

	return GPOS_NEW(pmp) CDXLPhysicalAgg(pmp, edxlaggstr, fStreamSafe);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopSort
//
//	@doc:
//		Construct a sort operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopSort
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse discard duplicates and nulls first properties from the attributes
	BOOL fDiscardDuplicates = FValueFromAttrs
								(
								pmm,
								attrs,
								EdxltokenSortDiscardDuplicates,
								EdxltokenPhysicalSort
								);
	
	return GPOS_NEW(pmp) CDXLPhysicalSort(pmp, fDiscardDuplicates);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopMaterialize
//
//	@doc:
//		Construct a materialize operator
//
//---------------------------------------------------------------------------
CDXLPhysical *
CDXLOperatorFactory::PdxlopMaterialize
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse spooling info from the attributes
	
	CDXLPhysicalMaterialize *pdxlopMat = NULL;
	
	// is this a multi-slice spool
	BOOL fEagerMaterialize = FValueFromAttrs
								(
								pmm,
								attrs,
								EdxltokenMaterializeEager,
								EdxltokenPhysicalMaterialize
								);
	
	if (1 == attrs.getLength())
	{
		// no spooling info specified -> create a non-spooling materialize operator
		pdxlopMat = GPOS_NEW(pmp) CDXLPhysicalMaterialize(pmp, fEagerMaterialize);
	}
	else
	{
		// parse spool id
		ULONG ulSpoolId = UlValueFromAttrs
							(
							pmm,
							attrs,
							EdxltokenSpoolId,
							EdxltokenPhysicalMaterialize
							);
	
		// parse id of executor slice
		INT iExecutorSlice = IValueFromAttrs
									(
									pmm,
									attrs,
									EdxltokenExecutorSliceId,
									EdxltokenPhysicalMaterialize
									);
		
		ULONG ulConsumerSlices = UlValueFromAttrs
									(
									pmm,
									attrs,
									EdxltokenConsumerSliceCount,
									EdxltokenPhysicalMaterialize
									);
	
		pdxlopMat = GPOS_NEW(pmp) CDXLPhysicalMaterialize(pmp, fEagerMaterialize, ulSpoolId, iExecutorSlice, ulConsumerSlices);
	}
	
	return pdxlopMat;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopScalarCmp
//
//	@doc:
//		Construct a scalar comparison operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopScalarCmp
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	 // get comparison operator from attributes
	const XMLCh *xmlszCmpOp = XmlstrFromAttrs(attrs, EdxltokenComparisonOp, EdxltokenScalarComp);
	
	// parse op no and function id
	IMDId *pmdidOpNo = PmdidFromAttrs(pmm, attrs, EdxltokenOpNo, EdxltokenScalarComp);
	
	// parse comparison operator from string
	CWStringDynamic *pstrCompOpName = CDXLUtils::PstrFromXMLCh(pmm, xmlszCmpOp);

	
	// copy dynamic string into const string
	CWStringConst *pstrCompOpNameCopy = GPOS_NEW(pmp) CWStringConst(pmp, pstrCompOpName->Wsz());

	// cleanup
	GPOS_DELETE(pstrCompOpName);
	
	return GPOS_NEW(pmp) CDXLScalarComp(pmp, pmdidOpNo, pstrCompOpNameCopy);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopDistinctCmp
//
//	@doc:
//		Construct a scalar distinct comparison operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopDistinctCmp
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse operator and function id
	IMDId *pmdidOpNo = PmdidFromAttrs(pmm, attrs, EdxltokenOpNo,EdxltokenScalarDistinctComp);

	return GPOS_NEW(pmp) CDXLScalarDistinctComp(pmp, pmdidOpNo);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopOpExpr
//
//	@doc:
//		Construct a scalar OpExpr
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopOpExpr
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	// get scalar OpExpr from attributes
	const XMLCh *xmlszOpName = XmlstrFromAttrs
									(
									attrs,
									EdxltokenOpName,
									EdxltokenScalarOpExpr
									);

	IMDId *pmdidOpNo = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenOpNo,
							EdxltokenScalarOpExpr
							);

	IMDId *pmdidReturnType = NULL;
	const XMLCh *xmlszReturnType = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenOpType));

	if (NULL != xmlszReturnType)
	{
		pmdidReturnType = PmdidFromAttrs(pmm, attrs, EdxltokenOpType, EdxltokenScalarOpExpr);
	}
	
	CWStringDynamic *pstrValue = CDXLUtils::PstrFromXMLCh(pmm, xmlszOpName);
	CWStringConst *pstrValueCopy = GPOS_NEW(pmp) CWStringConst(pmp, pstrValue->Wsz());
	GPOS_DELETE(pstrValue);

	return GPOS_NEW(pmp) CDXLScalarOpExpr(pmp, pmdidOpNo, pmdidReturnType, pstrValueCopy);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopArrayComp
//
//	@doc:
//		Construct a scalar array comparison
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopArrayComp
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	 // get attributes
	const XMLCh *xmlszOpName = XmlstrFromAttrs
								(
								attrs,
								EdxltokenOpName,
								EdxltokenScalarArrayComp
								);
	
	const XMLCh *xmlszOpType = XmlstrFromAttrs
									(
									attrs,
									EdxltokenOpType,
									EdxltokenScalarArrayComp
									);

	// parse operator no and function id
	IMDId *pmdidOpNo = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenOpNo,
							EdxltokenScalarArrayComp
							);

	EdxlArrayCompType edxlarraycomptype = Edxlarraycomptypeany;

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpTypeAll), xmlszOpType))
	{
		edxlarraycomptype = Edxlarraycomptypeall;
	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOpTypeAny), xmlszOpType))
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenOpType)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenScalarArrayComp)->Wsz()
			);
	}

	CWStringDynamic *pstrOpName = CDXLUtils::PstrFromXMLCh(pmm, xmlszOpName);
	CWStringConst *pstrOpNameCopy = GPOS_NEW(pmp) CWStringConst(pmp, pstrOpName->Wsz());
	GPOS_DELETE(pstrOpName);

	return GPOS_NEW(pmp) CDXLScalarArrayComp(pmp, pmdidOpNo, pstrOpNameCopy, edxlarraycomptype);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopBoolExpr
//
//	@doc:
//		Construct a scalar BoolExpr
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopBoolExpr
	(
	CDXLMemoryManager *pmm,
	const EdxlBoolExprType edxlboolexprType
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLScalarBoolExpr(pmp,	edxlboolexprType);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopBooleanTest
//
//	@doc:
//		Construct a scalar BooleanTest
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopBooleanTest
	(
	CDXLMemoryManager *pmm,
	const EdxlBooleanTestType edxlbooleantesttype
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLScalarBooleanTest(pmp,	edxlbooleantesttype);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopSubPlan
//
//	@doc:
//		Construct a SubPlan node
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopSubPlan
	(
	CDXLMemoryManager *pmm,
	IMDId *pmdid,
	DrgPdxlcr *pdrgdxlcr,
	EdxlSubPlanType edxlsubplantype,
	CDXLNode *pdxlnTestExpr
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	return GPOS_NEW(pmp) CDXLScalarSubPlan(pmp, pmdid, pdrgdxlcr, edxlsubplantype, pdxlnTestExpr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopNullTest
//
//	@doc:
//		Construct a scalar NullTest
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopNullTest
	(
	CDXLMemoryManager *pmm,
	const BOOL fIsNull
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	return GPOS_NEW(pmp) CDXLScalarNullTest(pmp, fIsNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopCast
//
//	@doc:
//		Construct a scalar RelabelType
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopCast
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse type id and function id
	IMDId *pmdidType = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenTypeId,
							EdxltokenScalarCast
							);

	IMDId *pmdidFunc = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenFuncId,
							EdxltokenScalarCast
							);

	return GPOS_NEW(pmp) CDXLScalarCast(pmp, pmdidType, pmdidFunc);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopCoerceToDomain
//
//	@doc:
//		Construct a scalar coerce
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopCoerceToDomain
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	// parse type id and function id
	IMDId *pmdidType = PmdidFromAttrs(pmm, attrs, EdxltokenTypeId, EdxltokenScalarCoerceToDomain);
	INT iTypeModifier = IValueFromAttrs(pmm, attrs, EdxltokenTypeMod, EdxltokenScalarCoerceToDomain, true, IDefaultTypeModifier);
	ULONG ulCoercionForm = UlValueFromAttrs(pmm, attrs, EdxltokenCoercionForm, EdxltokenScalarCoerceToDomain);
	INT iLoc = IValueFromAttrs(pmm, attrs, EdxltokenLocation, EdxltokenScalarCoerceToDomain);

	return GPOS_NEW(pmp) CDXLScalarCoerceToDomain(pmp, pmdidType, iTypeModifier, (EdxlCoercionForm) ulCoercionForm, iLoc);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopCoerceViaIO
//
//	@doc:
//		Construct a scalar coerce
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopCoerceViaIO
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	// parse type id and function id
	IMDId *pmdidType = PmdidFromAttrs(pmm, attrs, EdxltokenTypeId, EdxltokenScalarCoerceViaIO);
	INT iTypeModifier = IValueFromAttrs(pmm, attrs, EdxltokenTypeMod, EdxltokenScalarCoerceViaIO, true, IDefaultTypeModifier);
	ULONG ulCoercionForm = UlValueFromAttrs(pmm, attrs, EdxltokenCoercionForm, EdxltokenScalarCoerceViaIO);
	INT iLoc = IValueFromAttrs(pmm, attrs, EdxltokenLocation, EdxltokenScalarCoerceViaIO);

	return GPOS_NEW(pmp) CDXLScalarCoerceViaIO(pmp, pmdidType, iTypeModifier, (EdxlCoercionForm) ulCoercionForm, iLoc);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopArrayCoerceExpr
//
//	@doc:
//		Construct a scalar array coerce expression
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopArrayCoerceExpr
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	IMemoryPool *pmp = pmm->Pmp();

	IMDId *pmdidElementFunc = PmdidFromAttrs(pmm, attrs, EdxltokenElementFunc, EdxltokenScalarArrayCoerceExpr);
	IMDId *pmdidType = PmdidFromAttrs(pmm, attrs, EdxltokenTypeId, EdxltokenScalarArrayCoerceExpr);
	INT iTypeModifier = IValueFromAttrs(pmm, attrs, EdxltokenTypeMod, EdxltokenScalarArrayCoerceExpr, true, IDefaultTypeModifier);
	BOOL fIsExplicit = FValueFromAttrs(pmm, attrs, EdxltokenIsExplicit, EdxltokenScalarArrayCoerceExpr);
	ULONG ulCoercionForm = UlValueFromAttrs(pmm, attrs, EdxltokenCoercionForm, EdxltokenScalarArrayCoerceExpr);
	INT iLoc = IValueFromAttrs(pmm, attrs, EdxltokenLocation, EdxltokenScalarArrayCoerceExpr);

	return GPOS_NEW(pmp) CDXLScalarArrayCoerceExpr(pmp, pmdidElementFunc, pmdidType, iTypeModifier, fIsExplicit, (EdxlCoercionForm) ulCoercionForm, iLoc);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopConstValue
//
//	@doc:
//		Construct a scalar Const
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopConstValue
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	CDXLDatum *pdxldatum = Pdxldatum(pmm, attrs, EdxltokenScalarConstValue);
	
	return GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopIfStmt
//
//	@doc:
//		Construct an if statement operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopIfStmt
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	// get the type id
	IMDId *pmdidType = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenTypeId,
							EdxltokenScalarIfStmt
							);

	return GPOS_NEW(pmp) CDXLScalarIfStmt(pmp, pmdidType);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopFuncExpr
//
//	@doc:
//		Construct an funcexpr operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopFuncExpr
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	IMDId *pmdidFunc = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenFuncId,
							EdxltokenScalarFuncExpr
							);

	BOOL fRetset = FValueFromAttrs
					(
					pmm,
					attrs,
					EdxltokenFuncRetSet,
					EdxltokenScalarFuncExpr
					);

	IMDId *pmdidRetType = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenTypeId,
							EdxltokenScalarFuncExpr
							);

	INT iTypeModifier = IValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeMod,
						EdxltokenScalarCast,
						true,
						IDefaultTypeModifier
						);

	return GPOS_NEW(pmp) CDXLScalarFuncExpr(pmp, pmdidFunc, pmdidRetType, iTypeModifier, fRetset);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopAggFunc
//
//	@doc:
//		Construct an AggRef operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopAggFunc
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	IMDId *pmdidAgg = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenAggrefOid,
							EdxltokenScalarAggref
							);

	const XMLCh *xmlszStage  = XmlstrFromAttrs
									(
									attrs,
									EdxltokenAggrefStage,
									EdxltokenScalarAggref
									);
	
	BOOL fDistinct = FValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenAggrefDistinct,
						EdxltokenScalarAggref
						);

	EdxlAggrefStage edxlaggstage = EdxlaggstageFinal;

	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStageNormal), xmlszStage))
	{
		edxlaggstage = EdxlaggstageNormal;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStagePartial), xmlszStage))
	{
		edxlaggstage = EdxlaggstagePartial;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStageIntermediate), xmlszStage))
	{
		edxlaggstage = EdxlaggstageIntermediate;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenAggrefStageFinal), xmlszStage))
	{
		edxlaggstage = EdxlaggstageFinal;
	}
	else
	{
		// turn Xerces exception in optimizer exception	
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenAggrefStage)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenScalarAggref)->Wsz()
			);
	}

	IMDId *pmdidResolvedRetType = NULL;
	const XMLCh *xmlszReturnType = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenTypeId));
	if (NULL != xmlszReturnType)
	{
		pmdidResolvedRetType = PmdidFromAttrs
									(
									pmm,
									attrs,
									EdxltokenTypeId,
									EdxltokenScalarAggref
									);
	}

	return GPOS_NEW(pmp) CDXLScalarAggref(pmp, pmdidAgg, pmdidResolvedRetType, fDistinct, edxlaggstage);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Edxlfb
//
//	@doc:
//		Parse the frame boundary
//
//---------------------------------------------------------------------------
EdxlFrameBoundary
CDXLOperatorFactory::Edxlfb
	(
	const Attributes& attrs,
	Edxltoken edxltoken
	)
{
	const XMLCh *xmlszBoundary  = XmlstrFromAttrs(attrs, edxltoken, EdxltokenWindowFrame);

	EdxlFrameBoundary edxlfb = EdxlfbSentinel;
	ULONG rgrgulMapping[][2] =
					{
					{EdxlfbUnboundedPreceding, EdxltokenWindowBoundaryUnboundedPreceding},
					{EdxlfbBoundedPreceding, EdxltokenWindowBoundaryBoundedPreceding},
					{EdxlfbCurrentRow, EdxltokenWindowBoundaryCurrentRow},
					{EdxlfbUnboundedFollowing, EdxltokenWindowBoundaryUnboundedFollowing},
					{EdxlfbBoundedFollowing, EdxltokenWindowBoundaryBoundedFollowing},
					{EdxlfbDelayedBoundedPreceding, EdxltokenWindowBoundaryDelayedBoundedPreceding},
					{EdxlfbDelayedBoundedFollowing, EdxltokenWindowBoundaryDelayedBoundedFollowing}
					};

	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		Edxltoken edxltk = (Edxltoken) pulElem[1];
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(edxltk), xmlszBoundary))
		{
			edxlfb = (EdxlFrameBoundary) pulElem[0];
			break;
		}
	}

	if (EdxlfbSentinel == edxlfb)
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(edxltoken)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenWindowFrame)->Wsz()
			);
	}

	return edxlfb;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Edxlfs
//
//	@doc:
//		Parse the frame specification
//
//---------------------------------------------------------------------------
EdxlFrameSpec
CDXLOperatorFactory::Edxlfs
	(
	const Attributes& attrs
	)
{
	const XMLCh *xmlszfs  = XmlstrFromAttrs(attrs, EdxltokenWindowFrameSpec, EdxltokenWindowFrame);

	EdxlFrameSpec edxlfb = EdxlfsSentinel;
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFSRow), xmlszfs))
	{
		edxlfb = EdxlfsRow;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenWindowFSRange), xmlszfs))
	{
		edxlfb = EdxlfsRange;
	}
	else
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenWindowFrameSpec)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenWindowFrame)->Wsz()
			);
	}

	return edxlfb;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Edxlfes
//
//	@doc:
//		Parse the frame exclusion strategy
//
//---------------------------------------------------------------------------
EdxlFrameExclusionStrategy
CDXLOperatorFactory::Edxlfes
	(
	const Attributes& attrs
	)
{
	const XMLCh *xmlszfes  = XmlstrFromAttrs(attrs, EdxltokenWindowExclusionStrategy, EdxltokenWindowFrame);

	ULONG rgrgulMapping[][2] =
			{
			{EdxlfesNone, EdxltokenWindowESNone},
			{EdxlfesNulls, EdxltokenWindowESNulls},
			{EdxlfesCurrentRow, EdxltokenWindowESCurrentRow},
			{EdxlfesGroup, EdxltokenWindowESGroup},
			{EdxlfesTies, EdxltokenWindowESTies}
			};

	EdxlFrameExclusionStrategy edxlfes = EdxlfesSentinel;
	const ULONG ulArity = GPOS_ARRAY_SIZE(rgrgulMapping);
	for (ULONG ul = 0; ul < ulArity; ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		Edxltoken edxltk = (Edxltoken) pulElem[1];
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(edxltk), xmlszfes))
		{
			edxlfes = (EdxlFrameExclusionStrategy) pulElem[0];
			break;
		}
	}

	if (EdxlfesSentinel == edxlfes)
	{
		// turn Xerces exception in optimizer exception
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenWindowExclusionStrategy)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenWindowFrame)->Wsz()
			);
	}

	return edxlfes;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopArray
//
//	@doc:
//		Construct an array operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopArray
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	IMDId *pmdidElem = PmdidFromAttrs(pmm, attrs, EdxltokenArrayElementType, EdxltokenScalarArray);
	IMDId *pmdidArray = PmdidFromAttrs(pmm, attrs, EdxltokenArrayType, EdxltokenScalarArray);
	BOOL fMultiDimensional = FValueFromAttrs(pmm, attrs, EdxltokenArrayMultiDim, EdxltokenScalarArray);

	return GPOS_NEW(pmp) CDXLScalarArray(pmp, pmdidElem, pmdidArray, fMultiDimensional);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopScalarIdent
//
//	@doc:
//		Construct a scalar identifier operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopScalarIdent
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	CDXLColRef *pdxlcr = Pdxlcr(pmm, attrs, EdxltokenScalarIdent);

	return GPOS_NEW(pmp) CDXLScalarIdent(pmp, pdxlcr);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopProjElem
//
//	@doc:
//		Construct a proj elem operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopProjElem
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse alias from attributes	
	const XMLCh *xmlszAlias = XmlstrFromAttrs
								(
								attrs,
								EdxltokenAlias,
								EdxltokenScalarProjElem
								);
	
	// parse column id
	ULONG ulId = UlValueFromAttrs
					(
					pmm,
					attrs,
					EdxltokenColId,
					EdxltokenScalarProjElem
					);
	
	CWStringDynamic *pstrAlias = CDXLUtils::PstrFromXMLCh(pmm, xmlszAlias);

	// create a copy of the string in the CMDName constructor
	CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pstrAlias);
	
	GPOS_DELETE(pstrAlias);
	
	return GPOS_NEW(pmp) CDXLScalarProjElem(pmp, ulId, pmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopHashExpr
//
//	@doc:
//		Construct a hash expr operator
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopHashExpr
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// get column type id and type name from attributes

	IMDId *pmdidType = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenTypeId,
							EdxltokenScalarHashExpr
							);

	return GPOS_NEW(pmp) CDXLScalarHashExpr(pmp, pmdidType);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopSortCol
//
//	@doc:
//		Construct a sorting column description
//
//---------------------------------------------------------------------------
CDXLScalar *
CDXLOperatorFactory::PdxlopSortCol
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// get column id from attributes
	ULONG ulColId = UlValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenColId,
						EdxltokenScalarSortCol
						);

	// get sorting operator name
	const XMLCh *xmlszSortOpName = XmlstrFromAttrs
										(
										attrs,
										EdxltokenSortOpName,
										EdxltokenScalarSortCol
										);
	CWStringDynamic *pstrSortOpName = CDXLUtils::PstrFromXMLCh(pmm, xmlszSortOpName);

	// get null first property
	BOOL fNullsFirst = FValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenSortNullsFirst,
						EdxltokenPhysicalSort
						);
	
	// parse sorting operator id
	IMDId *pmdidSortOp = PmdidFromAttrs
								(
								pmm,
								attrs,
								EdxltokenSortOpId,
								EdxltokenPhysicalSort
								);

	// copy dynamic string into const string
	CWStringConst *pstrSortOpNameCopy = GPOS_NEW(pmp) CWStringConst(pmp, pstrSortOpName->Wsz());

	GPOS_DELETE(pstrSortOpName);

	return GPOS_NEW(pmp) CDXLScalarSortCol(pmp, ulColId, pmdidSortOp, pstrSortOpNameCopy, fNullsFirst);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pdxlopcost
//
//	@doc:
//		Construct a cost estimates element
//
//---------------------------------------------------------------------------
CDXLOperatorCost *
CDXLOperatorFactory::Pdxlopcost
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	const XMLCh *xmlszStartupCost = XmlstrFromAttrs
										(
										attrs,
										EdxltokenStartupCost,
										EdxltokenCost
										);
	
	const XMLCh *xmlszTotalCost = XmlstrFromAttrs
										(
										attrs,
										EdxltokenTotalCost,
										EdxltokenCost
										);
	
	const XMLCh *xmlszRows = XmlstrFromAttrs
								(
								attrs,
								EdxltokenRows,
								EdxltokenCost
								);
	
	const XMLCh *xmlszWidth = XmlstrFromAttrs
								(
								attrs,
								EdxltokenWidth,
								EdxltokenCost
								);
	
	CWStringDynamic *pstrStartupCost = CDXLUtils::PstrFromXMLCh(pmm, xmlszStartupCost);
	CWStringDynamic *pstrTotalCost = CDXLUtils::PstrFromXMLCh(pmm, xmlszTotalCost);
	CWStringDynamic *pstrRows = CDXLUtils::PstrFromXMLCh(pmm, xmlszRows);
	CWStringDynamic *pstrWidth = CDXLUtils::PstrFromXMLCh(pmm, xmlszWidth);
	
	return GPOS_NEW(pmp) CDXLOperatorCost
						(
						pstrStartupCost,
						pstrTotalCost,
						pstrRows,
						pstrWidth
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pdxltabdesc
//
//	@doc:
//		Construct a table descriptor
//
//---------------------------------------------------------------------------
CDXLTableDescr *
CDXLOperatorFactory::Pdxltabdesc
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse table descriptor from attributes
	const XMLCh *xmlszTableName = XmlstrFromAttrs
									(
									attrs,
									EdxltokenTableName,
									EdxltokenTableDescr
									);

	CMDName *pmdname = CDXLUtils::PmdnameFromXmlsz(pmm, xmlszTableName);
	
	// parse metadata id
	IMDId *pmdid = PmdidFromAttrs
						(
						pmm,
						attrs,
						EdxltokenMdid,
						EdxltokenTableDescr
						);

	// parse execute as user value if the attribute is specified
	ULONG ulUserId = GPDXL_DEFAULT_USERID;
	const XMLCh *xmlszExecuteAsUser = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenExecuteAsUser));

	if (NULL != xmlszExecuteAsUser)
	{
		ulUserId = UlValueFromXmlstr(pmm, xmlszExecuteAsUser, EdxltokenExecuteAsUser, EdxltokenTableDescr);
	}
					
	return GPOS_NEW(pmp) CDXLTableDescr(pmp, pmdid, pmdname, ulUserId);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pdxlid
//
//	@doc:
//		Construct an index descriptor
//
//---------------------------------------------------------------------------
CDXLIndexDescr *
CDXLOperatorFactory::Pdxlid
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	// parse index descriptor from attributes
	const XMLCh *xmlszIndexName = XmlstrFromAttrs
									(
									attrs,
									EdxltokenIndexName,
									EdxltokenIndexDescr
									);

	CWStringDynamic *pstrIndexName = CDXLUtils::PstrFromXMLCh(pmm, xmlszIndexName);

	// parse metadata id
	IMDId *pmdid = PmdidFromAttrs
						(
						pmm,
						attrs,
						EdxltokenMdid,
						EdxltokenIndexDescr
						);

	// create a copy of the string in the CMDName constructor
	CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pstrIndexName);
	GPOS_DELETE(pstrIndexName);

	return GPOS_NEW(pmp) CDXLIndexDescr(pmp, pmdid, pmdname);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pdxlcd
//
//	@doc:
//		Construct a column descriptor
//
//---------------------------------------------------------------------------
CDXLColDescr *
CDXLOperatorFactory::Pdxlcd
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	// parse column name from attributes
	const XMLCh *xmlszColumnName = XmlstrFromAttrs
										(
										attrs,
										EdxltokenColName,
										EdxltokenColDescr
										);

	// parse column id
	ULONG ulId = UlValueFromAttrs
					(
					pmm,
					attrs,
					EdxltokenColId,
					EdxltokenColDescr
					);
	
	// parse attno
	INT iAttno = IValueFromAttrs
					(
					pmm,
					attrs,
					EdxltokenAttno,
					EdxltokenColDescr
					);
	
	if (0 == iAttno)
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenAttno)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenColDescr)->Wsz()
			);
	}
	
	// parse column type id
	IMDId *pmdidType = PmdidFromAttrs
							(
							pmm,
							attrs,
							EdxltokenTypeId,
							EdxltokenColDescr
							);

	// parse optional type modifier from attributes
	INT iTypeModifier = IValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeMod,
						EdxltokenColDescr,
						true,
						IDefaultTypeModifier
						);

	BOOL fColDropped = false;
	
	const XMLCh *xmlszColDropped = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColDropped));
	
	if (NULL != xmlszColDropped)
	{
		// attribute is present: get value
		fColDropped = FValueFromXmlstr
						(
						pmm,
						xmlszColDropped,
						EdxltokenColDropped,
						EdxltokenColDescr
						);
	}

	ULONG ulColLen = gpos::ulong_max;

	// parse column length from attributes
	const XMLCh *xmlszColumnLength =  attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColWidth));

	if (NULL != xmlszColumnLength)
	{
		ulColLen = UlValueFromXmlstr
					(
					pmm,
					xmlszColumnLength,
					EdxltokenColWidth,
					EdxltokenColDescr
					);
	}

	CWStringDynamic *pstrColumnName = CDXLUtils::PstrFromXMLCh(pmm,xmlszColumnName);

	// create a copy of the string in the CMDName constructor
	CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pstrColumnName);
	
	GPOS_DELETE(pstrColumnName);
	
	return GPOS_NEW(pmp) CDXLColDescr(pmp, pmdname, ulId, iAttno, pmdidType, iTypeModifier, fColDropped, ulColLen);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pdxlcr
//
//	@doc:
//		Construct a column reference
//
//---------------------------------------------------------------------------
CDXLColRef *
CDXLOperatorFactory::Pdxlcr
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	// parse column name from attributes
	const XMLCh *xmlszColumnName = XmlstrFromAttrs
									(
									attrs,
									EdxltokenColName,
									edxltokenElement
									);

	// parse column id
	ULONG ulId = 0;
	const XMLCh *xmlszColumnId = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenColId));
	if(NULL == xmlszColumnId)
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLMissingAttribute,
			CDXLTokens::PstrToken(EdxltokenColRef)->Wsz(),
			CDXLTokens::PstrToken(edxltokenElement)->Wsz()
			);
	}
	
	ulId = XMLString::parseInt(xmlszColumnId, pmm);
		
	CWStringDynamic *pstrColumnName =  CDXLUtils::PstrFromXMLCh(pmm,xmlszColumnName);

	// create a copy of the string in the CMDName constructor
	CMDName *pmdname = GPOS_NEW(pmp) CMDName(pmp, pstrColumnName);
	
	GPOS_DELETE(pstrColumnName);

	IMDId *pmdidType = PmdidFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeId,
						edxltokenElement
						);

	// parse optional type modifier
	INT iTypeModifier = IValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeMod,
						edxltokenElement,
						true,
						IDefaultTypeModifier
						);
	
	return GPOS_NEW(pmp) CDXLColRef(pmp, pmdname, ulId, pmdidType, iTypeModifier);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::IOutputSegId
//
//	@doc:
//		Parse an output segment index
//
//---------------------------------------------------------------------------
INT
CDXLOperatorFactory::IOutputSegId
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get output segment index from attributes
	const XMLCh *xmlszSegId = XmlstrFromAttrs
									(
									attrs,
									EdxltokenSegId,
									EdxltokenSegment
									);

	// parse segment id from string
	INT iSegId = -1;
	try
	{
		iSegId = XMLString::parseInt(xmlszSegId, pmm);
	}
	catch (const NumberFormatException& toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenSegId)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenSegment)->Wsz()
			);
	}

	return iSegId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::XmlstrFromAttrs
//
//	@doc:
//    	Extracts the value for the given attribute.
// 		If there is no such attribute defined, and the given optional
// 		flag is set to false then it will raise an exception
//---------------------------------------------------------------------------
const XMLCh *
CDXLOperatorFactory::XmlstrFromAttrs
	(
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional
	)
{
	const XMLCh *xmlszAttributeVal = attrs.getValue(CDXLTokens::XmlstrToken(edxltokenAttr));

	if (NULL == xmlszAttributeVal && !fOptional)
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLMissingAttribute,
			CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
			CDXLTokens::PstrToken(edxltokenElement)->Wsz()
			);
	}

	return xmlszAttributeVal;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::UlValueFromXmlstr
//
//	@doc:
//	  	Converts the attribute value to ULONG
//
//---------------------------------------------------------------------------
ULONG
CDXLOperatorFactory::UlValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);
	ULONG ulId = 0;
	try
	{
		ulId = XMLString::parseInt(xmlszAttributeVal, pmm);
	}
	catch (const NumberFormatException& toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
		CDXLTokens::PstrToken(edxltokenElement)->Wsz()
		);
	}
	return ulId;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::UllValueFromXmlstr
//
//	@doc:
//	  	Converts the attribute value to ULLONG
//
//---------------------------------------------------------------------------
ULLONG
CDXLOperatorFactory::UllValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);

	CHAR *sz = XMLString::transcode(xmlszAttributeVal, pmm);
	GPOS_ASSERT(NULL != sz);

	CHAR **ppszEnd = NULL;
	LINT liVal = clib::LStrToLL(sz, ppszEnd, 10 /*ulBase*/);

	if ((NULL != ppszEnd && sz == *ppszEnd) ||
		gpos::lint_max == liVal || gpos::lint_min == liVal || 0 > liVal)
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
			CDXLTokens::PstrToken(edxltokenElement)->Wsz()
			);
	}

	XMLString::release(&sz, pmm);

	return (ULLONG) liVal;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::FValueFromXmlstr
//
//	@doc:
//	  	Converts the attribute value to BOOL
//
//---------------------------------------------------------------------------
BOOL
CDXLOperatorFactory::FValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);
	BOOL flag = false;
	CHAR *sz = XMLString::transcode(xmlszAttributeVal, pmm);

	if (0 == strncasecmp(sz, "true", 4))
	{
		flag = true;
	}else if (0 != strncasecmp(sz, "false", 5))
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
			CDXLTokens::PstrToken(edxltokenElement)->Wsz()
			);
	}

	XMLString::release(&sz, pmm);
	return flag;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::IValueFromXmlstr
//
//	@doc:
//	  	Converts the attribute value from xml string to INT
//
//---------------------------------------------------------------------------
INT
CDXLOperatorFactory::IValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);
	INT iId = 0;
	try
	{
		iId = XMLString::parseInt(xmlszAttributeVal, pmm);
	}
	catch (const NumberFormatException& toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
		CDXLTokens::PstrToken(edxltokenElement)->Wsz()
		);
	}
	return iId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::IValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into INT
//
//---------------------------------------------------------------------------
INT
CDXLOperatorFactory::IValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	INT iDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return iDefaultValue;
	}

	return IValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::SValueFromXmlstr
//
//	@doc:
//	  	Converts the attribute value from xml string to short int
//
//---------------------------------------------------------------------------
SINT
CDXLOperatorFactory::SValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);
	SINT sVal = 0;
	try
	{
		sVal = (SINT) XMLString::parseInt(xmlszAttributeVal, pmm);
	}
	catch (const NumberFormatException& toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
		CDXLTokens::PstrToken(edxltokenElement)->Wsz()
		);
	}
	return sVal;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::SValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into short
//		int
//
//---------------------------------------------------------------------------
SINT
CDXLOperatorFactory::SValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	SINT sDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return sDefaultValue;
	}

	return SValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

// Converts the attribute value from xml string to char
CHAR
CDXLOperatorFactory::CValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszVal,
	Edxltoken , // edxltokenAttr,
	Edxltoken  // edxltokenElement
	)
{
	GPOS_ASSERT(xmlszVal != NULL);
	CHAR *sz = XMLString::transcode(xmlszVal, pmm);
	CHAR val = *sz;
	XMLString::release(&sz, pmm);
	return val;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::OidValueFromXmlstr
//
//	@doc:
//	  	Converts the attribute value to OID
//
//---------------------------------------------------------------------------
OID
CDXLOperatorFactory::OidValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);
	OID oid = 0;
	try
	{
		oid = XMLString::parseInt(xmlszAttributeVal, pmm);
	}
	catch (const NumberFormatException& toCatch)
	{
		// turn Xerces exception into GPOS exception
		GPOS_RAISE
		(
		gpdxl::ExmaDXL,
		gpdxl::ExmiDXLInvalidAttributeValue,
		CDXLTokens::PstrToken(edxltokenAttr)->Wsz(),
		CDXLTokens::PstrToken(edxltokenElement)->Wsz()
		);
	}
	return oid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::OidValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into OID
//
//---------------------------------------------------------------------------
OID
CDXLOperatorFactory::OidValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	OID OidDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return OidDefaultValue;
	}

	return OidValueFromXmlstr(pmm, xmlszValue, edxltokenAttr, edxltokenElement);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::SzValueFromXmlstr
//
//	@doc:
//	  	Converts the string attribute value
//
//---------------------------------------------------------------------------
CHAR *
CDXLOperatorFactory::SzValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz,
	Edxltoken , // edxltokenAttr,
	Edxltoken  // edxltokenElement
	)
{
	GPOS_ASSERT(NULL != xmlsz);
	return XMLString::transcode(xmlsz, pmm);	
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::SzValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into CHAR*
//
//---------------------------------------------------------------------------
CHAR *
CDXLOperatorFactory::SzValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	CHAR *szDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return szDefaultValue;
	}

	return CDXLOperatorFactory::SzValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PstrValueFromAttrs
//
//	@doc:
//	  	Extracts the string value for the given attribute
//
//---------------------------------------------------------------------------
CWStringDynamic *
CDXLOperatorFactory::PstrValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement
											);
	return CDXLUtils::PstrFromXMLCh(pmm, xmlszValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::FValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into BOOL
//
//---------------------------------------------------------------------------
BOOL
CDXLOperatorFactory::FValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	BOOL fDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return fDefaultValue;
	}

	return FValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::UlValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into ULONG
//
//---------------------------------------------------------------------------
ULONG
CDXLOperatorFactory::UlValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	ULONG ulDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return ulDefaultValue;
	}

	return UlValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::UllValueFromAttrs
//
//	@doc:
//	  	Extracts the value for the given attribute and converts it into ULLONG
//
//---------------------------------------------------------------------------
ULLONG
CDXLOperatorFactory::UllValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	ULLONG ullDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement,
											fOptional
											);

	if (NULL == xmlszValue)
	{
		return ullDefaultValue;
	}

	return UllValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::UlGroupingColId
//
//	@doc:
//		Parse a grouping column id
//
//---------------------------------------------------------------------------
ULONG
CDXLOperatorFactory::UlGroupingColId
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	const CWStringConst *pstrTokenGroupingCol = CDXLTokens::PstrToken(EdxltokenGroupingCol);
	const CWStringConst *pstrTokenColId = CDXLTokens::PstrToken(EdxltokenColId);

	// get grouping column id from attributes	
	INT iColId = IValueFromAttrs(pmm, attrs, EdxltokenColId, EdxltokenGroupingCol);
	
	if (iColId < 0)
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			pstrTokenColId->Wsz(),
			pstrTokenGroupingCol->Wsz()
			);	
	}
	
	return (ULONG) iColId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidFromAttrs
//
//	@doc:
//		Parse a metadata id object from the XML attributes of the specified element.
//
//---------------------------------------------------------------------------
IMDId *
CDXLOperatorFactory::PmdidFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	IMDId *pmdidDefault
	)
{
	// extract mdid
	const XMLCh *xmlszMdid = XmlstrFromAttrs(attrs, edxltokenAttr, edxltokenElement, fOptional);

	if (NULL == xmlszMdid)
	{
		if (NULL != pmdidDefault)
		{
			pmdidDefault->AddRef();
		}

		return pmdidDefault;
	}
	
	return PmdidFromXMLCh(pmm, xmlszMdid, edxltokenAttr, edxltokenElement);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidFromXMLCh
//
//	@doc:
//		Parse a metadata id object from the XML attributes of the specified element.
//
//---------------------------------------------------------------------------
IMDId *
CDXLOperatorFactory::PmdidFromXMLCh
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszMdid,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	// extract mdid's components: MdidType.Oid.Major.Minor
	XMLStringTokenizer xmlsztok(xmlszMdid, CDXLTokens::XmlstrToken(EdxltokenDotSemicolon));

	GPOS_ASSERT(1 < xmlsztok.countTokens());
	
	// get mdid type from first component
	XMLCh *xmlszMdidType = xmlsztok.nextToken();
	
	// collect the remaining tokens in an array
	DrgPxmlsz *pdrgpxmlsz = GPOS_NEW(pmm->Pmp()) DrgPxmlsz(pmm->Pmp());
	
	XMLCh *xmlsz = NULL;
	while (NULL != (xmlsz = xmlsztok.nextToken()))
	{
		pdrgpxmlsz->Append(xmlsz);
	}
	
	IMDId::EMDIdType emdt = (IMDId::EMDIdType) UlValueFromXmlstr(pmm, xmlszMdidType, edxltokenAttr, edxltokenElement);

	IMDId *pmdid = NULL;
	switch (emdt)
	{
		case IMDId::EmdidGPDB:
			pmdid = PmdidGPDB(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
			break;
			
		case IMDId::EmdidGPDBCtas:
			pmdid = PmdidGPDBCTAS(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
			break;
			
		case IMDId::EmdidColStats:
			pmdid = PmdidColStats(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
			break;
			
		case IMDId::EmdidRelStats:
			pmdid = PmdidRelStats(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
			break;
			
		case IMDId::EmdidCastFunc:
			pmdid = PmdidCastFunc(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
			break;
			
		case IMDId::EmdidScCmp:
			pmdid = PmdidScCmp(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
			break;
			
		default:
			GPOS_ASSERT(!"Unrecognized mdid type");		
	}
	
	pdrgpxmlsz->Release();
	
	return pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidGPDB
//
//	@doc:
//		Construct a GPDB mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdGPDB *
CDXLOperatorFactory::PmdidGPDB
	(
	CDXLMemoryManager *pmm,
	DrgPxmlsz *pdrgpxmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS <= pdrgpxmlsz->UlLength());

	XMLCh *xmlszOid = (*pdrgpxmlsz)[0];
	ULONG ulOid = UlValueFromXmlstr(pmm, xmlszOid, edxltokenAttr, edxltokenElement);

	XMLCh *xmlszVersionMajor = (*pdrgpxmlsz)[1];
	ULONG ulVersionMajor = UlValueFromXmlstr(pmm, xmlszVersionMajor, edxltokenAttr, edxltokenElement);

	XMLCh *xmlszVersionMinor = (*pdrgpxmlsz)[2];;
	ULONG ulVersionMinor = UlValueFromXmlstr(pmm, xmlszVersionMinor, edxltokenAttr, edxltokenElement);

	// construct metadata id object
	return GPOS_NEW(pmm->Pmp()) CMDIdGPDB(ulOid, ulVersionMajor, ulVersionMinor);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidGPDBCTAS
//
//	@doc:
//		Construct a GPDB CTAS mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdGPDB *
CDXLOperatorFactory::PmdidGPDBCTAS
	(
	CDXLMemoryManager *pmm,
	DrgPxmlsz *pdrgpxmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS <= pdrgpxmlsz->UlLength());

	XMLCh *xmlszOid = (*pdrgpxmlsz)[0];
	ULONG ulOid = UlValueFromXmlstr(pmm, xmlszOid, edxltokenAttr, edxltokenElement);

	// construct metadata id object
	return GPOS_NEW(pmm->Pmp()) CMDIdGPDBCtas(ulOid);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidColStats
//
//	@doc:
//		Construct a column stats mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdColStats *
CDXLOperatorFactory::PmdidColStats
	(
	CDXLMemoryManager *pmm,
	DrgPxmlsz *pdrgpxmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS + 1 == pdrgpxmlsz->UlLength());

	CMDIdGPDB *pmdidRel = PmdidGPDB(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
	
	XMLCh *xmlszAttno = (*pdrgpxmlsz)[3];
	ULONG ulAttno = UlValueFromXmlstr(pmm, xmlszAttno, edxltokenAttr, edxltokenElement);

	// construct metadata id object
	return GPOS_NEW(pmm->Pmp()) CMDIdColStats(pmdidRel, ulAttno);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidRelStats
//
//	@doc:
//		Construct a relation stats mdid from an array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdRelStats *
CDXLOperatorFactory::PmdidRelStats
	(
	CDXLMemoryManager *pmm,
	DrgPxmlsz *pdrgpxmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(GPDXL_GPDB_MDID_COMPONENTS == pdrgpxmlsz->UlLength());

	CMDIdGPDB *pmdidRel = PmdidGPDB(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
	
	// construct metadata id object
	return GPOS_NEW(pmm->Pmp()) CMDIdRelStats(pmdidRel);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidCastFunc
//
//	@doc:
//		Construct a cast function mdid from the array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdCast *
CDXLOperatorFactory::PmdidCastFunc
	(
	CDXLMemoryManager *pmm,
	DrgPxmlsz *pdrgpxmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(2 * GPDXL_GPDB_MDID_COMPONENTS == pdrgpxmlsz->UlLength());

	CMDIdGPDB *pmdidSrc = PmdidGPDB(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
	DrgPxmlsz *pdrgpxmlszDest = GPOS_NEW(pmm->Pmp()) DrgPxmlsz(pmm->Pmp());
	
	for (ULONG ul = GPDXL_GPDB_MDID_COMPONENTS; ul < GPDXL_GPDB_MDID_COMPONENTS * 2; ul++)
	{
		pdrgpxmlszDest->Append((*pdrgpxmlsz)[ul]);
	}
	
	CMDIdGPDB *pmdidDest = PmdidGPDB(pmm, pdrgpxmlszDest, edxltokenAttr, edxltokenElement);
	pdrgpxmlszDest->Release();
	
	return GPOS_NEW(pmm->Pmp()) CMDIdCast(pmdidSrc, pmdidDest);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PmdidScCmp
//
//	@doc:
//		Construct a scalar comparison operator mdid from the array of XML string components.
//
//---------------------------------------------------------------------------
CMDIdScCmp *
CDXLOperatorFactory::PmdidScCmp
	(
	CDXLMemoryManager *pmm,
	DrgPxmlsz *pdrgpxmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	GPOS_ASSERT(2 * GPDXL_GPDB_MDID_COMPONENTS + 1 == pdrgpxmlsz->UlLength());

	CMDIdGPDB *pmdidLeft = PmdidGPDB(pmm, pdrgpxmlsz, edxltokenAttr, edxltokenElement);
	DrgPxmlsz *pdrgpxmlszRight = GPOS_NEW(pmm->Pmp()) DrgPxmlsz(pmm->Pmp());
	
	for (ULONG ul = GPDXL_GPDB_MDID_COMPONENTS; ul < GPDXL_GPDB_MDID_COMPONENTS * 2 + 1; ul++)
	{
		pdrgpxmlszRight->Append((*pdrgpxmlsz)[ul]);
	}
	
	CMDIdGPDB *pmdidRight = PmdidGPDB(pmm, pdrgpxmlszRight, edxltokenAttr, edxltokenElement);
	
	// parse the comparison type from the last component of the mdid
	XMLCh *xmlszCmpType = (*pdrgpxmlszRight)[pdrgpxmlszRight->UlLength() - 1];
	IMDType::ECmpType ecmpt = (IMDType::ECmpType) UlValueFromXmlstr(pmm, xmlszCmpType, edxltokenAttr, edxltokenElement);
	GPOS_ASSERT(IMDType::EcmptOther > ecmpt);
	
	pdrgpxmlszRight->Release();
	
	return GPOS_NEW(pmm->Pmp()) CMDIdScCmp(pmdidLeft, pmdidRight, ecmpt);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pdxldatum
//
//	@doc:
//		Parses a DXL datum from the given attributes
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::Pdxldatum
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement
	)
{	
	// get the type id and value of the datum from attributes
	IMDId *pmdid = PmdidFromAttrs(pmm, attrs, EdxltokenTypeId, edxltokenElement);
	GPOS_ASSERT(IMDId::EmdidGPDB == pmdid->Emdidt());
	CMDIdGPDB *pmdidgpdbd = CMDIdGPDB::PmdidConvert(pmdid);

	// get the type id from string
	BOOL fConstNull = FValueFromAttrs(pmm, attrs, EdxltokenIsNull, edxltokenElement);
	BOOL fConstByVal = FValueFromAttrs(pmm, attrs, EdxltokenIsByValue, edxltokenElement);

	
	SDXLDatumFactoryElem rgTranslators[] =
	{
		// native support
		{CMDIdGPDB::m_mdidInt2.OidObjectId() , &CDXLOperatorFactory::PdxldatumInt2},
		{CMDIdGPDB::m_mdidInt4.OidObjectId() , &CDXLOperatorFactory::PdxldatumInt4},
		{CMDIdGPDB::m_mdidInt8.OidObjectId() , &CDXLOperatorFactory::PdxldatumInt8},
		{CMDIdGPDB::m_mdidBool.OidObjectId() , &CDXLOperatorFactory::PdxldatumBool},
		{CMDIdGPDB::m_mdidOid.OidObjectId() , &CDXLOperatorFactory::PdxldatumOid},
		// types with long int mapping
		{CMDIdGPDB::m_mdidBPChar.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsLintMappable},
		{CMDIdGPDB::m_mdidVarChar.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsLintMappable},
		{CMDIdGPDB::m_mdidText.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsLintMappable},
		{CMDIdGPDB::m_mdidCash.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsLintMappable},
		// non-integer numeric types
		{CMDIdGPDB::m_mdidNumeric.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidFloat4.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidFloat8.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		// network-related types
		{CMDIdGPDB::m_mdidInet.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidCidr.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidMacaddr.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		// time-related types
		{CMDIdGPDB::m_mdidDate.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidTime.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidTimeTz.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidTimestamp.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidTimestampTz.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidAbsTime.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidRelTime.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidInterval.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable},
		{CMDIdGPDB::m_mdidTimeInterval.OidObjectId(), &CDXLOperatorFactory::PdxldatumStatsDoubleMappable}
	};
	
	const ULONG ulTranslators = GPOS_ARRAY_SIZE(rgTranslators);
	// find translator for the datum type
	PfPdxldatum *pf = NULL;
	for (ULONG ul = 0; ul < ulTranslators; ul++)
	{
		SDXLDatumFactoryElem elem = rgTranslators[ul];
		if (pmdidgpdbd->OidObjectId() == elem.oid)
		{
			pf = elem.pf;
			break;
		}
	}
		
	if (NULL == pf)
	{
		// generate a datum of generic type
		return PdxldatumGeneric(pmm, attrs, edxltokenElement, pmdid, fConstNull, fConstByVal);
	}
	else
	{
		return (*pf)(pmm, attrs, edxltokenElement, pmdid, fConstNull, fConstByVal);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumOid
//
//	@doc:
//		Parses a DXL datum of oid type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumOid
(
 CDXLMemoryManager *pmm,
 const Attributes &attrs,
 Edxltoken edxltokenElement,
 IMDId *pmdid,
 BOOL fConstNull ,
 BOOL
#ifdef GPOS_DEBUG
 fConstByVal
#endif // GPOS_DEBUG
 )
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	GPOS_ASSERT(fConstByVal);
	OID oVal = 0;
	if (!fConstNull)
	{
		oVal = OidValueFromAttrs(pmm, attrs, EdxltokenValue, edxltokenElement);
	}

	return GPOS_NEW(pmp) CDXLDatumOid(pmp, pmdid, fConstNull, oVal);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumInt2
//
//	@doc:
//		Parses a DXL datum of int2 type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumInt2
(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement,
	IMDId *pmdid,
	BOOL fConstNull ,
	BOOL
	#ifdef GPOS_DEBUG
	fConstByVal
	#endif // GPOS_DEBUG
 )
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	GPOS_ASSERT(fConstByVal);
	SINT sVal = 0;
	if (!fConstNull)
	{
		sVal = SValueFromAttrs(pmm, attrs, EdxltokenValue, edxltokenElement);
	}

	return GPOS_NEW(pmp) CDXLDatumInt2(pmp, pmdid, fConstNull, sVal);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumInt4
//
//	@doc:
//		Parses a DXL datum of int4 type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumInt4
(
 CDXLMemoryManager *pmm,
 const Attributes &attrs,
 Edxltoken edxltokenElement,
 IMDId *pmdid,
 BOOL fConstNull ,
 BOOL
#ifdef GPOS_DEBUG
 fConstByVal
#endif // GPOS_DEBUG
 )
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	GPOS_ASSERT(fConstByVal);
	INT iVal = 0;
	if (!fConstNull)
	{
		iVal = IValueFromAttrs(pmm, attrs, EdxltokenValue, edxltokenElement);
	}
	
	return GPOS_NEW(pmp) CDXLDatumInt4(pmp, pmdid, fConstNull, iVal);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumInt8
//
//	@doc:
//		Parses a DXL datum of int8 type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumInt8
(
 CDXLMemoryManager *pmm,
 const Attributes &attrs,
 Edxltoken edxltokenElement,
 IMDId *pmdid,
 BOOL fConstNull ,
 BOOL
#ifdef GPOS_DEBUG
 fConstByVal
#endif // GPOS_DEBUG
 )
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	GPOS_ASSERT(fConstByVal);
	LINT lVal = 0;
	if (!fConstNull)
	{
		lVal = LValueFromAttrs(pmm, attrs, EdxltokenValue, edxltokenElement);
	}
	
	return GPOS_NEW(pmp) CDXLDatumInt8(pmp, pmdid, fConstNull, lVal);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumBool
//
//	@doc:
//		Parses a DXL datum of boolean type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumBool
(
 CDXLMemoryManager *pmm,
 const Attributes &attrs,
 Edxltoken edxltokenElement,
 IMDId *pmdid,
 BOOL fConstNull ,
 BOOL
#ifdef GPOS_DEBUG
 fConstByVal
#endif // GPOS_DEBUG
 )
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	GPOS_ASSERT(fConstByVal);
	BOOL fVal = false;
	if (!fConstNull)
	{
		fVal = FValueFromAttrs(pmm, attrs, EdxltokenValue, edxltokenElement);
	}
	
	return GPOS_NEW(pmp) CDXLDatumBool(pmp, pmdid, fConstNull, fVal);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumGeneric
//
//	@doc:
//		Parses a DXL datum of generic type
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumGeneric
(
 CDXLMemoryManager *pmm,
 const Attributes &attrs,
 Edxltoken edxltokenElement,
 IMDId *pmdid,
 BOOL fConstNull ,
 BOOL fConstByVal
 )
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	ULONG ulPbaLength = 0;
	BYTE *pba = NULL;
	
	if (!fConstNull)
	{
		pba = Pba(pmm, attrs, edxltokenElement, &ulPbaLength);
		if (NULL == pba)
		{
			// unable to decode value. probably not Base64 encoded.
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLInvalidAttributeValue, CDXLTokens::XmlstrToken(EdxltokenValue), CDXLTokens::PstrToken(edxltokenElement));
		}
	}

	INT iTypeModifier = IValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeMod,
						EdxltokenScalarCast,
						true,
						IDefaultTypeModifier
						);

	return GPOS_NEW(pmp) CDXLDatumGeneric(pmp, pmdid, iTypeModifier, fConstByVal, fConstNull, pba, ulPbaLength);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumStatsLintMappable
//
//	@doc:
//		Parses a DXL datum of types having lint mapping
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumStatsLintMappable
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement,
	IMDId *pmdid,
	BOOL fConstNull ,
	BOOL fConstByVal
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	ULONG ulPbaLength = 0;
	BYTE *pba = NULL;

	LINT lValue = 0;
	if (!fConstNull)
	{
		pba = Pba(pmm, attrs, edxltokenElement, &ulPbaLength);
		lValue = LValue(pmm, attrs, edxltokenElement, pba);
	}

	INT iTypeModifier = IValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeMod,
						EdxltokenScalarCast,
						true,
						-1 /* default value */
						);

	return GPOS_NEW(pmp) CDXLDatumStatsLintMappable(pmp, pmdid, iTypeModifier, fConstByVal, fConstNull, pba, ulPbaLength, lValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::LValue
//
//	@doc:
//		Return the LINT value of byte array
//
//---------------------------------------------------------------------------
LINT
CDXLOperatorFactory::LValue
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement,
	BYTE *pba
	)
{
	if (NULL == pba)
	{
		// unable to decode value. probably not Base64 encoded.
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLInvalidAttributeValue, CDXLTokens::XmlstrToken(EdxltokenValue), CDXLTokens::PstrToken(edxltokenElement));
	}

	return LValueFromAttrs(pmm, attrs, EdxltokenLintValue, edxltokenElement);
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Pba
//
//	@doc:
//		Parses a byte array representation of the datum
//
//---------------------------------------------------------------------------
BYTE *
CDXLOperatorFactory::Pba
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement,
	ULONG *pulLength
	)
{
	const XMLCh *xmlszValue  = XmlstrFromAttrs(attrs, EdxltokenValue, edxltokenElement);

	return CDXLUtils::PbaFromBase64XMLStr(pmm, xmlszValue, pulLength);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxldatumStatsDoubleMappable
//
//	@doc:
//		Parses a DXL datum of types that need double mapping
//
//---------------------------------------------------------------------------
CDXLDatum *
CDXLOperatorFactory::PdxldatumStatsDoubleMappable
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenElement,
	IMDId *pmdid,
	BOOL fConstNull ,
	BOOL fConstByVal
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	ULONG ulPbaLength = 0;
	BYTE *pba = NULL;
	CDouble dValue = 0;
	
	if (!fConstNull)
	{
		pba = Pba(pmm, attrs, edxltokenElement, &ulPbaLength);
		
		if (NULL == pba)
		{
			// unable to decode value. probably not Base64 encoded.
			GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLInvalidAttributeValue, CDXLTokens::XmlstrToken(EdxltokenValue), CDXLTokens::PstrToken(edxltokenElement));
		}

		dValue = DValueFromAttrs(pmm, attrs, EdxltokenDoubleValue, edxltokenElement);
	}
	INT iTypeModifier = IValueFromAttrs
						(
						pmm,
						attrs,
						EdxltokenTypeMod,
						EdxltokenScalarCast,
						true,
						-1 /* default value */
						);
	return GPOS_NEW(pmp) CDXLDatumStatsDoubleMappable(pmp, pmdid, iTypeModifier, fConstByVal, fConstNull, pba, ulPbaLength, dValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdrgpulFromAttrs
//
//	@doc:
//		Parse a comma-separated list of unsigned long integers ids into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
DrgPul *
CDXLOperatorFactory::PdrgpulFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	const XMLCh *xmlsz = CDXLOperatorFactory::XmlstrFromAttrs(attrs, edxltokenAttr, edxltokenElement);

	return PdrgpulFromXMLCh(pmm, xmlsz, edxltokenAttr, edxltokenElement);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdrgpmdidFromXMLCh
//
//	@doc:
//		Parse a comma-separated list of MDids into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
DrgPmdid *
CDXLOperatorFactory::PdrgpmdidFromXMLCh
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszUlList,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	DrgPmdid *pdrgpmdid = GPOS_NEW(pmp) DrgPmdid(pmp);

	XMLStringTokenizer xmlsztok(xmlszUlList, CDXLTokens::XmlstrToken(EdxltokenComma));
	const ULONG ulNumTokens = xmlsztok.countTokens();

	for (ULONG ul = 0; ul < ulNumTokens; ul++)
	{
		XMLCh *xmlszMdid = xmlsztok.nextToken();
		GPOS_ASSERT(NULL != xmlszMdid);

		IMDId *pmdid = PmdidFromXMLCh(pmm, xmlszMdid, edxltokenAttr, edxltokenElement);
		pdrgpmdid->Append(pmdid);
	}

	return pdrgpmdid;
}

// Parse a comma-separated list of CHAR partition types into a dynamic array.
// Will raise an exception if list is not well-formed
DrgPsz *
CDXLOperatorFactory::PdrgpszFromXMLCh
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	return PdrgptFromXMLCh<CHAR, CleanupDelete, CValueFromXmlstr>
			(
			pmm,
			xmlsz,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdrgpdrgpulFromXMLCh
//
//	@doc:
//		Parse a semicolon-separated list of comma-separated unsigned long 
//		integers into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
DrgPdrgPul *
CDXLOperatorFactory::PdrgpdrgpulFromXMLCh
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
		
	DrgPdrgPul *pdrgpdrgpul = GPOS_NEW(pmp) DrgPdrgPul(pmp);
	
	XMLStringTokenizer xmlsztok(xmlsz, CDXLTokens::XmlstrToken(EdxltokenSemicolon));
	const ULONG ulNumTokens = xmlsztok.countTokens();
	
	for (ULONG ul = 0; ul < ulNumTokens; ul++)
	{
		XMLCh *xmlszList = xmlsztok.nextToken();
		
		GPOS_ASSERT(NULL != xmlszList);
		
		DrgPul *pdrgpul = PdrgpulFromXMLCh(pmm, xmlszList, edxltokenAttr, edxltokenElement);
		pdrgpdrgpul->Append(pdrgpul);
	}
	
	return pdrgpdrgpul;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdrgpiParseSegmentIdList
//
//	@doc:
//		Parse a comma-separated list of segment ids into a dynamic array.
//		Will raise an exception if list is not well-formed
//
//---------------------------------------------------------------------------
DrgPi *
CDXLOperatorFactory::PdrgpiParseSegmentIdList
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszSegIdList,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	
	GPOS_ASSERT(NULL != xmlszSegIdList);
	
	DrgPi *pdrgpiSegIds = GPOS_NEW(pmp) DrgPi(pmp);
	
	XMLStringTokenizer xmlsztok(xmlszSegIdList, CDXLTokens::XmlstrToken(EdxltokenComma));
	
	const ULONG ulNumSegments = xmlsztok.countTokens();
	GPOS_ASSERT(0 < ulNumSegments);
	
	for (ULONG ul = 0; ul < ulNumSegments; ul++)
	{
		XMLCh *xmlszSegId = xmlsztok.nextToken();
		
		GPOS_ASSERT(NULL != xmlszSegId);
		
		INT *piSegId = GPOS_NEW(pmp) INT(IValueFromXmlstr(pmm, xmlszSegId, edxltokenAttr, edxltokenElement));
		pdrgpiSegIds->Append(piSegId);
	}
	
	return pdrgpiSegIds;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdrgPstrFromXMLCh
//
//	@doc:
//		Parse a semicolon-separated list of strings into a dynamic array.
//
//---------------------------------------------------------------------------
DrgPstr *
CDXLOperatorFactory::PdrgPstrFromXMLCh
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlsz
	)
{
	IMemoryPool *pmp = pmm->Pmp();

	DrgPstr *pdrgpstr = GPOS_NEW(pmp) DrgPstr(pmp);

	XMLStringTokenizer xmlsztok(xmlsz, CDXLTokens::XmlstrToken(EdxltokenSemicolon));
	const ULONG ulNumTokens = xmlsztok.countTokens();

	for (ULONG ul = 0; ul < ulNumTokens; ul++)
	{
		XMLCh *xmlszString = xmlsztok.nextToken();
		GPOS_ASSERT(NULL != xmlszString);

		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(pmm, xmlszString);
		pdrgpstr->Append(pstr);
	}

	return pdrgpstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::SetSegmentInfo
//
//	@doc:
//		Parses the input and output segment ids from Xerces attributes and
//		stores them in the provided DXL Motion operator.
//		Will raise an exception if lists are not well-formed 
//
//---------------------------------------------------------------------------
void
CDXLOperatorFactory::SetSegmentInfo
	(
	CDXLMemoryManager *pmm,
	CDXLPhysicalMotion *pdxlopMotion,
	const Attributes &attrs,
	Edxltoken edxltokenElement
	)
{
	const XMLCh *xmlszInputSegList = XmlstrFromAttrs
										(
										attrs,
										EdxltokenInputSegments,
										edxltokenElement
										);
	DrgPi *pdrgpiInputSegments = PdrgpiParseSegmentIdList
									(
									pmm,
									xmlszInputSegList,
									EdxltokenInputSegments,
									edxltokenElement
									);
	pdxlopMotion->SetInputSegIds(pdrgpiInputSegments);

	const XMLCh *xmlszOutputSegList = XmlstrFromAttrs
										(
										attrs,
										EdxltokenOutputSegments,
										edxltokenElement
										);
	DrgPi *pdrgpiOutputSegments = PdrgpiParseSegmentIdList
									(
									pmm,
									xmlszOutputSegList,
									EdxltokenOutputSegments,
									edxltokenElement
									);
	pdxlopMotion->SetOutputSegIds(pdrgpiOutputSegments);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::EdxljtParseJoinType
//
//	@doc:
//		Parse a join type from the attribute value.
//		Raise an exception if join type value is invalid.
//
//---------------------------------------------------------------------------
EdxlJoinType
CDXLOperatorFactory::EdxljtParseJoinType
	(
	const XMLCh *xmlszJoinType,
	const CWStringConst *pstrJoinName
	)
{
	EdxlJoinType edxljt = EdxljtSentinel;
	
	if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinInner),
			xmlszJoinType))
	{
		edxljt = EdxljtInner;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinLeft),
					xmlszJoinType))
	{
		edxljt = EdxljtLeft;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinFull),
			xmlszJoinType))
	{
		edxljt = EdxljtFull;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinRight),
				xmlszJoinType))
	{
		edxljt = EdxljtRight;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinIn),
					xmlszJoinType))
	{
		edxljt = EdxljtIn;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinLeftAntiSemiJoin),
					xmlszJoinType))
	{
		edxljt = EdxljtLeftAntiSemijoin;
	}
	else if(0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenJoinLeftAntiSemiJoinNotIn),
					xmlszJoinType))
	{
		edxljt = EdxljtLeftAntiSemijoinNotIn;
	}
	else
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenJoinType)->Wsz(),
			pstrJoinName->Wsz()
			);		
	}
	
	return edxljt;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::EdxljtParseIndexScanDirection
//
//	@doc:
//		Parse the index scan direction from the attribute value. Raise
//		exception if it is invalid
//
//---------------------------------------------------------------------------
EdxlIndexScanDirection
CDXLOperatorFactory::EdxljtParseIndexScanDirection
	(
	const XMLCh *xmlszIndexScanDirection,
	const CWStringConst *pstrIndexScan
	)
{
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexScanDirectionBackward),
			xmlszIndexScanDirection))
	{
		return EdxlisdBackward;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexScanDirectionForward),
					xmlszIndexScanDirection))
	{
		return EdxlisdForward;
	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenIndexScanDirectionNoMovement),
			xmlszIndexScanDirection))
	{
		return EdxlisdNoMovement;
	}
	else
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenIndexScanDirection)->Wsz(),
			pstrIndexScan->Wsz()
			);
	}

	return EdxlisdSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopLogicalJoin
//
//	@doc:
//		Construct a logical join operator
//
//---------------------------------------------------------------------------
CDXLLogical*
CDXLOperatorFactory::PdxlopLogicalJoin
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();

	const XMLCh *xmlszJoinType = XmlstrFromAttrs(attrs, EdxltokenJoinType, EdxltokenLogicalJoin);
	EdxlJoinType edxljt = EdxljtParseJoinType(xmlszJoinType, CDXLTokens::PstrToken(EdxltokenLogicalJoin));

	return GPOS_NEW(pmp) CDXLLogicalJoin(pmp, edxljt);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::DValueFromXmlstr
//
//	@doc:
//	  Converts the attribute value to CDouble
//
//---------------------------------------------------------------------------
CDouble
CDXLOperatorFactory::DValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken ,//edxltokenAttr,
	Edxltoken //edxltokenElement
	)
{
	GPOS_ASSERT(xmlszAttributeVal != NULL);
	CHAR *sz = XMLString::transcode(xmlszAttributeVal, pmm);

	CDouble dValue(clib::DStrToD(sz));

	XMLString::release(&sz, pmm);
	return dValue;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::DValueFromAttrs
//
//	@doc:
//	  Extracts the value for the given attribute and converts it into CDouble
//
//---------------------------------------------------------------------------
CDouble
CDXLOperatorFactory::DValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs
											(
											attrs,
											edxltokenAttr,
											edxltokenElement
											);
	return DValueFromXmlstr
			(
			pmm,
			xmlszValue,
			edxltokenAttr,
			edxltokenElement
			);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::LValueFromXmlstr
//
//	@doc:
//	  Converts the attribute value to LINT
//
//---------------------------------------------------------------------------
LINT
CDXLOperatorFactory::LValueFromXmlstr
	(
	CDXLMemoryManager *pmm,
	const XMLCh *xmlszAttributeVal,
	Edxltoken ,//edxltokenAttr,
	Edxltoken //edxltokenElement
	)
{
	GPOS_ASSERT(NULL != xmlszAttributeVal);
	CHAR *sz = XMLString::transcode(xmlszAttributeVal, pmm);
	CHAR *szEnd = NULL;

	LINT lValue = clib::LStrToLL(sz, &szEnd, 10);
	XMLString::release(&sz, pmm);

	return lValue;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::LValueFromAttrs
//
//	@doc:
//	  Extracts the value for the given attribute and converts it into LINT
//
//---------------------------------------------------------------------------
LINT
CDXLOperatorFactory::LValueFromAttrs
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement,
	BOOL fOptional,
	LINT lDefaultValue
	)
{
	const XMLCh *xmlszValue = CDXLOperatorFactory::XmlstrFromAttrs(attrs, edxltokenAttr, edxltokenElement, fOptional);

	if (NULL == xmlszValue)
	{
		return lDefaultValue;
	}

	return LValueFromXmlstr(pmm, xmlszValue, edxltokenAttr, edxltokenElement);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Sysid
//
//	@doc:
//	  Extracts the value for the given attribute and converts it into CDouble
//
//---------------------------------------------------------------------------
CSystemId
CDXLOperatorFactory::Sysid
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs,
	Edxltoken edxltokenAttr,
	Edxltoken edxltokenElement
	)
{
	// extract systemids
	const XMLCh *xmlsz = XmlstrFromAttrs(attrs, edxltokenAttr, edxltokenElement);

	// get sysid components
	XMLStringTokenizer xmlsztokSysid(xmlsz, CDXLTokens::XmlstrToken(EdxltokenDot));
	GPOS_ASSERT(2 == xmlsztokSysid.countTokens());
	
	XMLCh *xmlszType = xmlsztokSysid.nextToken();
	ULONG ulType = CDXLOperatorFactory::UlValueFromXmlstr(pmm, xmlszType, edxltokenAttr, edxltokenElement);
	
	XMLCh *xmlszName = xmlsztokSysid.nextToken();
	CWStringDynamic *pstrName = CDXLUtils::PstrFromXMLCh(pmm, xmlszName);
	
	CSystemId sysid((IMDId::EMDIdType) ulType, pstrName->Wsz(), pstrName->UlLength());	
	GPOS_DELETE(pstrName);
	
	return sysid;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::PdxlopWindowRef
//
//	@doc:
//		Construct an WindowRef operator
//
//---------------------------------------------------------------------------
CDXLScalar*
CDXLOperatorFactory::PdxlopWindowRef
	(
	CDXLMemoryManager *pmm,
	const Attributes &attrs
	)
{
	// get the memory pool from the memory manager
	IMemoryPool *pmp = pmm->Pmp();
	IMDId *pmdidFunc = PmdidFromAttrs(pmm, attrs, EdxltokenWindowrefOid, EdxltokenScalarWindowref);
	IMDId *pmdidRetType = PmdidFromAttrs(pmm, attrs, EdxltokenTypeId, EdxltokenScalarWindowref);
	BOOL fDistinct = FValueFromAttrs(pmm, attrs, EdxltokenWindowrefDistinct, EdxltokenScalarWindowref);
	BOOL fStarArg = FValueFromAttrs(pmm, attrs, EdxltokenWindowrefStarArg, EdxltokenScalarWindowref);
	BOOL fSimpleAgg = FValueFromAttrs(pmm, attrs, EdxltokenWindowrefSimpleAgg, EdxltokenScalarWindowref);
	ULONG ulWinspecPos = UlValueFromAttrs(pmm, attrs, EdxltokenWindowrefWinSpecPos, EdxltokenScalarWindowref);

	const XMLCh *xmlszStage  = XmlstrFromAttrs(attrs, EdxltokenWindowrefStrategy, EdxltokenScalarWindowref);
	EdxlWinStage edxlwinstage = EdxlwinstageSentinel;

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
		Edxltoken edxltk = (Edxltoken) pulElem[1];
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(edxltk), xmlszStage))
		{
			edxlwinstage = (EdxlWinStage) pulElem[0];
			break;
		}
	}
	GPOS_ASSERT(EdxlwinstageSentinel != edxlwinstage);

	return GPOS_NEW(pmp) CDXLScalarWindowRef(pmp, pmdidFunc, pmdidRetType, fDistinct, fStarArg, fSimpleAgg, edxlwinstage, ulWinspecPos);
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::Ecmpt
//
//	@doc:
//		Parse comparison type
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CDXLOperatorFactory::Ecmpt
	(
	const XMLCh* xmlszCmpType
	)
{
	ULONG rgrgulMapping[][2] = 
	{
	{EdxltokenCmpEq, IMDType::EcmptEq},
	{EdxltokenCmpNeq,IMDType::EcmptNEq},
	{EdxltokenCmpLt, IMDType::EcmptL},
	{EdxltokenCmpLeq, IMDType::EcmptLEq},
	{EdxltokenCmpGt, IMDType::EcmptG},
	{EdxltokenCmpGeq,IMDType::EcmptGEq}, 
	{EdxltokenCmpIDF, IMDType::EcmptIDF},
	{EdxltokenCmpOther, IMDType::EcmptOther}
	};
	
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgrgulMapping); ul++)
	{
		ULONG *pulElem = rgrgulMapping[ul];
		Edxltoken edxltk = (Edxltoken) pulElem[0];
		
		if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(edxltk),
				xmlszCmpType))
		{
			return (IMDType::ECmpType) pulElem[1];
		}
	}
	
	GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLInvalidAttributeValue, CDXLTokens::PstrToken(EdxltokenGPDBScalarOpCmpType)->Wsz(), CDXLTokens::PstrToken(EdxltokenGPDBScalarOp)->Wsz());		
	return (IMDType::ECmpType) IMDType::EcmptOther;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::EreldistrpolicyFromXmlstr
//
//	@doc:
//		Parse relation distribution policy from XML string
//
//---------------------------------------------------------------------------
IMDRelation::Ereldistrpolicy
CDXLOperatorFactory::EreldistrpolicyFromXmlstr
	(
	const XMLCh* xmlsz
	)
{
	GPOS_ASSERT(NULL != xmlsz);
	IMDRelation::Ereldistrpolicy ereldistrpolicy = IMDRelation::EreldistrSentinel;
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelDistrMasterOnly)))
	{
		ereldistrpolicy = IMDRelation::EreldistrMasterOnly;
	}
	else if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelDistrHash)))
	{
		ereldistrpolicy = IMDRelation::EreldistrHash;
	}
	else if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelDistrRandom)))
	{
		ereldistrpolicy = IMDRelation::EreldistrRandom;
	}
	
	return ereldistrpolicy;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::ErelstoragetypeFromXmlstr
//
//	@doc:
//		Parse relation storage type from XML string
//
//---------------------------------------------------------------------------
IMDRelation::Erelstoragetype
CDXLOperatorFactory::ErelstoragetypeFromXmlstr
	(
	const XMLCh* xmlsz
	)
{
	GPOS_ASSERT(NULL != xmlsz);
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelStorageHeap)))
	{
		return IMDRelation::ErelstorageHeap;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelStorageAppendOnlyCols)))
	{
		return IMDRelation::ErelstorageAppendOnlyCols;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelStorageAppendOnlyRows)))
	{
		return IMDRelation::ErelstorageAppendOnlyRows;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelStorageAppendOnlyParquet)))
	{
		return IMDRelation::ErelstorageAppendOnlyParquet;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelStorageExternal)))
	{
		return IMDRelation::ErelstorageExternal;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenRelStorageVirtual)))
	{
		return IMDRelation::ErelstorageVirtual;
	}
	
	GPOS_ASSERT(!"Unrecognized storage type");
	
	return IMDRelation::ErelstorageSentinel;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::EctascommitFromAttr
//
//	@doc:
//		Parse on commit action spec from XML attributes
//
//---------------------------------------------------------------------------
CDXLCtasStorageOptions::ECtasOnCommitAction
CDXLOperatorFactory::EctascommitFromAttr
	(
	const Attributes &attrs
	)
{
	const XMLCh *xmlsz = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenOnCommitAction));

	if (NULL == xmlsz)
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLMissingAttribute,
			CDXLTokens::PstrToken(EdxltokenOnCommitAction)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenCTASOptions)->Wsz()
			);
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenOnCommitPreserve)))
	{
		return CDXLCtasStorageOptions::EctascommitPreserve;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenOnCommitDelete)))
	{
		return CDXLCtasStorageOptions::EctascommitDelete;
	}
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenOnCommitDrop)))
	{
		return CDXLCtasStorageOptions::EctascommitDrop;
	}
	
	if (0 != XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenOnCommitNOOP)))
	{
		GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenOnCommitAction)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenCTASOptions)->Wsz()
			);	
	}
	
	return CDXLCtasStorageOptions::EctascommitNOOP;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLOperatorFactory::EmdindtFromAttr
//
//	@doc:
//		Parse index type from XML attributes
//
//---------------------------------------------------------------------------
IMDIndex::EmdindexType
CDXLOperatorFactory::EmdindtFromAttr
	(
	const Attributes &attrs
	)
{
	const XMLCh *xmlsz = attrs.getValue(CDXLTokens::XmlstrToken(EdxltokenIndexType));

	if (NULL == xmlsz || 0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenIndexTypeBtree)))
	{
		// default is btree
		return IMDIndex::EmdindBtree;
	}
	
	
	if (0 == XMLString::compareString(xmlsz, CDXLTokens::XmlstrToken(EdxltokenIndexTypeBitmap)))
	{
		return IMDIndex::EmdindBitmap;
	}
	
	GPOS_RAISE
			(
			gpdxl::ExmaDXL,
			gpdxl::ExmiDXLInvalidAttributeValue,
			CDXLTokens::PstrToken(EdxltokenIndexType)->Wsz(),
			CDXLTokens::PstrToken(EdxltokenIndex)->Wsz()
			);	
	
	return IMDIndex::EmdindSentinel;
}

// EOF
