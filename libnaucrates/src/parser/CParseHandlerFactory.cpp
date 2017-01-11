//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010-2011 EMC Corp.
//
//	@filename:
//		CParseHandlerFactory.cpp
//
//	@doc:
//		Implementation of the factory methods for creating parse handlers
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/parsehandlers.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"
#include "naucrates/dxl/xml/dxltokens.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

CParseHandlerFactory::HMXMLStrPfPHCreator *
CParseHandlerFactory::m_phmPHCreators = NULL;

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::AddMapping
//
//	@doc:
//		Adds a new mapping of token to corresponding parse handler
//
//---------------------------------------------------------------------------
void
CParseHandlerFactory::AddMapping
	(
	Edxltoken edxltok,
	PfParseHandlerOpCreator *pfphopc
	)
{
	GPOS_ASSERT(NULL != m_phmPHCreators);
	const XMLCh *xmlszTok = CDXLTokens::XmlstrToken(edxltok);
	GPOS_ASSERT(NULL != xmlszTok);
	
#ifdef GPOS_DEBUG
	BOOL fInserted = 
#endif
	m_phmPHCreators->FInsert(xmlszTok, pfphopc);
	
	GPOS_ASSERT(fInserted);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::Init
//
//	@doc:
//		Initialize mapping of tokens to parse handlers
//
//---------------------------------------------------------------------------
void
CParseHandlerFactory::Init
	(
	IMemoryPool *pmp
	)
{
	m_phmPHCreators = GPOS_NEW(pmp) HMXMLStrPfPHCreator(pmp, ulHashMapSize);
	
	// array mapping XML Token -> Parse Handler Creator mappings to hashmap
	SParseHandlerMapping rgParseHandlers[] =
	{
			{EdxltokenPlan, &PphPlan},
			{EdxltokenMetadata, &PphMetadata},
			{EdxltokenMDRequest, &PphMDRequest},
			{EdxltokenTraceFlags, &PphTraceFlags},
			{EdxltokenOptimizerConfig, &PphOptimizerConfig},
			{EdxltokenRelationExternal, &PphMetadataRelationExternal},
			{EdxltokenRelationCTAS, &PphMetadataRelationCTAS},
			{EdxltokenEnumeratorConfig, &PphEnumeratorConfig},
			{EdxltokenStatisticsConfig, &PphStatisticsConfig},
			{EdxltokenCTEConfig, &PphCTEConfig},
			{EdxltokenCostModelConfig, &PphCostModelConfig},
			{EdxltokenHint, &PphHint},

			{EdxltokenRelation, &PphMetadataRelation},
			{EdxltokenIndex, &PphMDIndex},
			{EdxltokenMDType, &PphMDGPDBType},
			{EdxltokenGPDBScalarOp, &PphMDGPDBScalarOp},
			{EdxltokenGPDBFunc, &PphMDGPDBFunc},
			{EdxltokenGPDBAgg, &PphMDGPDBAgg},
			{EdxltokenGPDBTrigger, &PphMDGPDBTrigger},
			{EdxltokenCheckConstraint, &PphMDGPDBCheckConstraint},
			{EdxltokenRelationStats, &PphRelStats},
			{EdxltokenColumnStats, &PphColStats},
			{EdxltokenMetadataIdList, &PphMetadataIdList},
			{EdxltokenMetadataColumns, &PphMetadataColumns},
			{EdxltokenMetadataColumn, &PphMetadataColumn},
			{EdxltokenColumnDefaultValue, &PphColumnDefaultValueExpr},
			{EdxltokenColumnStatsBucket, &PphColStatsBucket},
			{EdxltokenGPDBCast, &PphMDCast},
			{EdxltokenGPDBMDScCmp, &PphMDScCmp},

			{EdxltokenPhysical, &PphPhysOp},

			{EdxltokenPhysicalAggregate, &PphAgg},
			{EdxltokenPhysicalTableScan, &PphTableScan},
			{EdxltokenPhysicalBitmapTableScan, &PphBitmapTableScan},
			{EdxltokenPhysicalDynamicBitmapTableScan, &PphDynamicBitmapTableScan},
			{EdxltokenPhysicalExternalScan, &PphExternalScan},
			{EdxltokenPhysicalHashJoin, &PphHashJoin},
			{EdxltokenPhysicalNLJoin, &PphNLJoin},
			{EdxltokenPhysicalMergeJoin, &PphMergeJoin},
			{EdxltokenPhysicalGatherMotion, &PphGatherMotion},
			{EdxltokenPhysicalBroadcastMotion, &PphBroadcastMotion},
			{EdxltokenPhysicalRedistributeMotion, &PphRedistributeMotion},
			{EdxltokenPhysicalRoutedDistributeMotion, &PphRoutedMotion},
			{EdxltokenPhysicalRandomMotion, &PphRandomMotion},
			{EdxltokenPhysicalSubqueryScan, &PphSubqScan},
			{EdxltokenPhysicalResult, &PphResult},
			{EdxltokenPhysicalLimit, &PphLimit},
			{EdxltokenPhysicalSort, &PphSort},
			{EdxltokenPhysicalAppend, &PphAppend},
			{EdxltokenPhysicalSharedScan, &PphSharedScan},
			{EdxltokenPhysicalMaterialize, &PphMaterialize},
		 	{EdxltokenPhysicalDynamicTableScan, &PphDynamicTableScan},
		 	{EdxltokenPhysicalDynamicIndexScan, &PphDynamicIndexScan},
		 	{EdxltokenPhysicalPartitionSelector, &PphPartitionSelector},
			{EdxltokenPhysicalSequence, &PphSequence},
			{EdxltokenPhysicalIndexScan, &PphIndexScan},
			{EdxltokenPhysicalIndexOnlyScan, &PphIndexOnlyScan},
			{EdxltokenScalarBitmapIndexProbe, &PphBitmapIndexProbe},
			{EdxltokenIndexDescr, &PphIndexDescr},

			{EdxltokenPhysicalWindow, &PphWindow},
			{EdxltokenScalarWindowref, &PphWindowRef},
			{EdxltokenWindowFrame, &PphWindowFrame},
			{EdxltokenScalarWindowFrameLeadingEdge, &PphWindowFrameLeadingEdge},
			{EdxltokenScalarWindowFrameTrailingEdge, &PphWindowFrameTrailingEdge},
			{EdxltokenWindowKey, &PphWindowKey},
			{EdxltokenWindowKeyList, &PphWindowKeyList},

			{EdxltokenScalarIndexCondList, &PphIndexCondList},
 
			{EdxltokenScalar, &PphScalarOp},

			{EdxltokenScalarFilter, &PphFilter},
			{EdxltokenScalarOneTimeFilter, &PphFilter},
			{EdxltokenScalarRecheckCondFilter, &PphFilter},
			{EdxltokenScalarProjList, &PphProjList},
			{EdxltokenScalarProjElem, &PphProjElem},
			{EdxltokenScalarAggref, &PphAggref},
			{EdxltokenScalarSortColList, &PphSortColList},
			{EdxltokenScalarSortCol, &PphSortCol},
			{EdxltokenScalarCoalesce, &PphScalarCoalesce},
			{EdxltokenScalarComp, &PphScalarCmp},
			{EdxltokenScalarDistinctComp, &PphDistinctCmp},
			{EdxltokenScalarIdent, &PphScalarId},
			{EdxltokenScalarOpExpr, &PphScalarOpexpr},
			{EdxltokenScalarArrayComp, &PphScalarArrayCmp},
			{EdxltokenScalarBoolOr, &PphScalarBoolExpr},
			{EdxltokenScalarBoolNot, &PphScalarBoolExpr},
			{EdxltokenScalarBoolAnd, &PphScalarBoolExpr},
			{EdxltokenScalarMin, &PphScalarMinMax},
			{EdxltokenScalarMax, &PphScalarMinMax},
			{EdxltokenScalarBoolTestIsTrue, &PphBooleanTest},
			{EdxltokenScalarBoolTestIsNotTrue, &PphBooleanTest},
			{EdxltokenScalarBoolTestIsFalse, &PphBooleanTest},
			{EdxltokenScalarBoolTestIsNotFalse, &PphBooleanTest},
			{EdxltokenScalarBoolTestIsUnknown, &PphBooleanTest},
			{EdxltokenScalarBoolTestIsNotUnknown, &PphBooleanTest},
			{EdxltokenScalarInitPlan, &PphScalarInitPlan},
			{EdxltokenScalarSubPlan, &PphScalarSubPlan},
			{EdxltokenScalarConstValue, &PphScalarConstValue},
			{EdxltokenScalarIfStmt, &PphIfStmt},
			{EdxltokenScalarSwitch, &PphScalarSwitch},
			{EdxltokenScalarSwitchCase, &PphScalarSwitchCase},
			{EdxltokenScalarCaseTest, &PphScalarCaseTest},
			{EdxltokenScalarFuncExpr, &PphScalarFuncExpr},
			{EdxltokenScalarIsNull, &PphScalarNullTest},
			{EdxltokenScalarIsNotNull, &PphScalarNullTest},
			{EdxltokenScalarNullIf, &PphScalarNullIf},
			{EdxltokenScalarCast, &PphScalarCast},
			{EdxltokenScalarCoerceToDomain, PphScalarCoerceToDomain},
			{EdxltokenScalarCoerceViaIO, PphScalarCoerceViaIO},
			{EdxltokenScalarArrayCoerceExpr, PphScalarArrayCoerceExpr},
			{EdxltokenScalarHashExpr, &PphHashExpr},
			{EdxltokenScalarHashCondList, &PphCondList},
			{EdxltokenScalarMergeCondList, &PphCondList},
			{EdxltokenScalarHashExprList, &PphHashExprList},
			{EdxltokenScalarGroupingColList, &PphGroupingColList},
			{EdxltokenScalarLimitOffset, &PphLimitoffset},
			{EdxltokenScalarLimitCount, &PphLimitcount},
			{EdxltokenScalarSubPlanTestExpr, &PphScalarSubPlanTestExpr},
			{EdxltokenScalarSubPlanParamList, &PphScalarSubPlanParamList},
			{EdxltokenScalarSubPlanParam, &PphScalarSubPlanParam},
			{EdxltokenScalarOpList, &PphScalarOpList},
			{EdxltokenScalarPartOid, &PphScalarPartOid},
			{EdxltokenScalarPartDefault, &PphScalarPartDefault},
			{EdxltokenScalarPartBound, &PphScalarPartBound},
			{EdxltokenScalarPartBoundInclusion, &PphScalarPartBoundInclusion},
			{EdxltokenScalarPartBoundOpen, &PphScalarPartBoundOpen},

			{EdxltokenScalarSubquery, &PphScalarSubquery},
			{EdxltokenScalarBitmapAnd, &PphScalarBitmapBoolOp},
			{EdxltokenScalarBitmapOr, &PphScalarBitmapBoolOp},

			{EdxltokenScalarArray, &PphScalarArray},
			{EdxltokenScalarArrayRef, &PphScalarArrayRef},
			{EdxltokenScalarArrayRefIndexList, &PphScalarArrayRefIndexList},
			
			{EdxltokenScalarAssertConstraintList, &PphScalarAssertConstraintList},
			
			{EdxltokenScalarDMLAction, &PphScalarDMLAction},
			{EdxltokenDirectDispatchInfo, &PphDirectDispatchInfo},

			{EdxltokenQueryOutput, &PphQueryOutput},

			{EdxltokenCost, &PphCost},
			{EdxltokenTableDescr, &PphTableDesc},
			{EdxltokenColumns, &PphColDesc},
			{EdxltokenProperties, &PphProperties},
			{EdxltokenPhysicalTVF, &PphPhysicalTVF},
			{EdxltokenLogicalTVF, &PphLogicalTVF},

			{EdxltokenQuery, &PphQuery},
			{EdxltokenLogicalGet, &PphLgGet},
			{EdxltokenLogicalExternalGet, &PphLgExternalGet},
			{EdxltokenLogical, &PphLgOp},
			{EdxltokenLogicalProject, &PphLgProject},
			{EdxltokenLogicalSelect, &PphLgSelect},
			{EdxltokenLogicalJoin, &PphLgJoin},
			{EdxltokenLogicalGrpBy, &PphLgGrpBy},
			{EdxltokenLogicalLimit, &PphLgLimit},
			{EdxltokenLogicalConstTable, &PphLgConstTable},
			{EdxltokenLogicalCTEProducer, &PphLgCTEProducer},
			{EdxltokenLogicalCTEConsumer, &PphLgCTEConsumer},
			{EdxltokenLogicalCTEAnchor, &PphLgCTEAnchor},
			{EdxltokenCTEList, &PphCTEList},

			{EdxltokenLogicalWindow, &PphLgWindow},
			{EdxltokenWindowSpec, &PphWindowSpec},
			{EdxltokenWindowSpecList, &PphWindowSpecList},

			{EdxltokenLogicalInsert, &PphLgInsert},
			{EdxltokenLogicalDelete, &PphLgDelete},
			{EdxltokenLogicalUpdate, &PphLgUpdate},
			{EdxltokenPhysicalDMLInsert, &PphPhDML},
			{EdxltokenPhysicalDMLDelete, &PphPhDML},
			{EdxltokenPhysicalDMLUpdate, &PphPhDML},
			{EdxltokenPhysicalSplit, &PphPhSplit},
			{EdxltokenPhysicalRowTrigger, &PphPhRowTrigger},
			{EdxltokenPhysicalAssert, &PphPhAssert},
			{EdxltokenPhysicalCTEProducer, &PphPhCTEProducer},
			{EdxltokenPhysicalCTEConsumer, &PphPhCTEConsumer},
			{EdxltokenLogicalCTAS, &PphLgCTAS},
			{EdxltokenPhysicalCTAS, &PphPhCTAS},
			{EdxltokenCTASOptions, &PphCTASOptions},
		
			{EdxltokenScalarSubqueryAny, &PphScSubqueryQuantified},
			{EdxltokenScalarSubqueryAll, &PphScSubqueryQuantified},
 			{EdxltokenScalarSubqueryExists, &PphScSubqueryExists},
			{EdxltokenScalarSubqueryNotExists, &PphScSubqueryExists},

			{EdxltokenStackTrace, &PphStacktrace},
			{EdxltokenLogicalUnion, &PphLgSetOp},
			{EdxltokenLogicalUnionAll, &PphLgSetOp},
			{EdxltokenLogicalIntersect, &PphLgSetOp},
			{EdxltokenLogicalIntersectAll, &PphLgSetOp},
			{EdxltokenLogicalDifference, &PphLgSetOp},
			{EdxltokenLogicalDifferenceAll, &PphLgSetOp},

			{EdxltokenStatistics, &PphStats},
			{EdxltokenStatsDerivedColumn, &PphStatsDerivedColumn},
			{EdxltokenStatsDerivedRelation, &PphStatsDerivedRelation},
			{EdxltokenStatsBucketLowerBound, &PphStatsBucketBound},
			{EdxltokenStatsBucketUpperBound, &PphStatsBucketBound},

			{EdxltokenSearchStrategy, &PphSearchStrategy},
			{EdxltokenSearchStage, &PphSearchStage},
			{EdxltokenXform, &PphXform},

			{EdxltokenCostParams, &PphCostParams},
			{EdxltokenCostParam, &PphCostParam},

			{EdxltokenScalarExpr, &PphScalarExpr}

	};
	
	const ULONG ulParsehandlers = GPOS_ARRAY_SIZE(rgParseHandlers);

	for (ULONG ul = 0; ul < ulParsehandlers; ul++)
	{
		SParseHandlerMapping elem = rgParseHandlers[ul];
		AddMapping(elem.edxltoken, elem.pfphopc);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::Pph
//
//	@doc:
//		Creates a parse handler instance given an xml tag
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::Pph
	(
	IMemoryPool *pmp,
	const XMLCh *xmlszName,
	CParseHandlerManager* pphm,
	CParseHandlerBase *pphRoot
	)
{
	GPOS_ASSERT(NULL != m_phmPHCreators);

	PfParseHandlerOpCreator *phoc = m_phmPHCreators->PtLookup(xmlszName);

	if (phoc != NULL)
	{
		return (*phoc) (pmp, pphm, pphRoot);
	}
	
	CDXLMemoryManager mm(pmp);

	// did not find the physical operator in the table
	CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(&mm, xmlszName);;

	GPOS_RAISE
	(
	gpdxl::ExmaDXL,
	gpdxl::ExmiDXLUnrecognizedOperator,
	pstr->Wsz()
	);

	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::Pphdxl
//
//	@doc:
//		Creates a parse handler for parsing a DXL document.
//
//---------------------------------------------------------------------------
CParseHandlerDXL *
CParseHandlerFactory::Pphdxl
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm
	)
{
	return GPOS_NEW(pmp) CParseHandlerDXL(pmp, pphm);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPlan
//
//	@doc:
//		Creates a parse handler for parsing a Plan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPlan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPlan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadata
//
//	@doc:
//		Creates a parse handler for parsing metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadata
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMetadata(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDRequest
//
//	@doc:
//		Creates a parse handler for parsing a metadata request
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDRequest
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDRequest(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphTraceFlags
//
//	@doc:
//		Creates a parse handler for parsing trace flags
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphTraceFlags
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerTraceFlags(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphOptimizerConfig
//
//	@doc:
//		Creates a parse handler for parsing optimizer config
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphOptimizerConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerOptimizerConfig(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphEnumeratorConfig
//
//	@doc:
//		Creates a parse handler for parsing enumerator config
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphEnumeratorConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerEnumeratorConfig(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphStatisticsConfig
//
//	@doc:
//		Creates a parse handler for parsing statistics configuration
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphStatisticsConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerStatisticsConfig(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCTEConfig
//
//	@doc:
//		Creates a parse handler for parsing CTE configuration
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCTEConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCTEConfig(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCostModelConfig
//
//	@doc:
//		Creates a parse handler for parsing cost model configuration
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCostModelConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCostModel(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphHint
//
//	@doc:
//		Creates a parse handler for parsing hint configuration
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphHint
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerHint(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadataRelation
//
//	@doc:
//		Creates a parse handler for parsing relation metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadataRelation
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDRelation(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadataRelationExternal
//
//	@doc:
//		Creates a parse handler for parsing external relation metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadataRelationExternal
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDRelationExternal(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadataRelationCTAS
//
//	@doc:
//		Creates a parse handler for parsing CTAS relation metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadataRelationCTAS
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDRelationCtas(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDIndex
//
//	@doc:
//		Creates a parse handler for parsing a MD index
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDIndex
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDIndex(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphRelStats
//
//	@doc:
//		Creates a parse handler for parsing relation stats
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphRelStats
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerRelStats(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphColStats
//
//	@doc:
//		Creates a parse handler for parsing column stats
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphColStats
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerColStats(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphColStatsBucket
//
//	@doc:
//		Creates a parse handler for parsing column stats bucket
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphColStatsBucket
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerColStatsBucket(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDGPDBType
//
//	@doc:
//		Creates a parse handler for parsing GPDB type metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDGPDBType
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDType(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDGPDBScalarOp
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific operator metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDGPDBScalarOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDGPDBScalarOp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDGPDBFunc
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific function metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDGPDBFunc
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDGPDBFunc(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDGPDBAgg
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific aggregate metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDGPDBAgg
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDGPDBAgg(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDGPDBTrigger
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific trigger metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDGPDBTrigger
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDGPDBTrigger(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDCast
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific cast metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDCast
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDCast(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDScCmp
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific scalar comparison metadata
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDScCmp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDScCmp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadataIdList
//
//	@doc:
//		Creates a parse handler for parsing a list of metadata identifiers
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadataIdList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMetadataIdList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadataColumns
//
//	@doc:
//		Creates a parse handler for parsing a list of column metadata info
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadataColumns
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMetadataColumns(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMetadataColumn
//
//	@doc:
//		Creates a parse handler for parsing column info
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMetadataColumn
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMetadataColumn(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphColumnDefaultValueExpr
//
//	@doc:
//		Creates a parse handler for parsing a a default value for a column
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphColumnDefaultValueExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerDefaultValueExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhysOp
//
//	@doc:
//		Creates a parse handler for parsing a physical operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhysOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalOp(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarOp
//
//	@doc:
//		Creates a parse handler for parsing a scalar operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarOp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphProperties
//
//	@doc:
//		Creates a parse handler for parsing the properties of a physical operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphProperties
	(
		IMemoryPool *pmp,
		CParseHandlerManager *pphm,
		CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerProperties(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphFilter
//
//	@doc:
//		Creates a parse handler for parsing a filter operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphFilter
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerFilter(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphTableScan
//
//	@doc:
//		Creates a parse handler for parsing a table scan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphTableScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerTableScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphBitmapTableScan
//
//	@doc:
//		Creates a parse handler for parsing a bitmap table scan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphBitmapTableScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalBitmapTableScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphDynamicBitmapTableScan
//
//	@doc:
//		Creates a parse handler for parsing a dynamic bitmap table scan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphDynamicBitmapTableScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalDynamicBitmapTableScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphExternalScan
//
//	@doc:
//		Creates a parse handler for parsing an external scan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphExternalScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerExternalScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSubqScan
//
//	@doc:
//		Creates a parse handler for parsing a subquery scan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSubqScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSubqueryScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphResult
//
//	@doc:
//		Creates a parse handler for parsing a result node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphResult
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerResult(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphHashJoin
//
//	@doc:
//		Creates a parse handler for parsing a hash join operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphHashJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerHashJoin(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphNLJoin
//
//	@doc:
//		Creates a parse handler for parsing a nested loop join operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphNLJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerNLJoin(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMergeJoin
//
//	@doc:
//		Creates a parse handler for parsing a merge join operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMergeJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMergeJoin(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSort
//
//	@doc:
//		Creates a parse handler for parsing a sort operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSort
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSort(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphAppend
//
//	@doc:
//		Creates a parse handler for parsing an append operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphAppend
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerAppend(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSharedScan
//
//	@doc:
//		Creates a parse handler for parsing a shared scan operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSharedScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSharedScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMaterialize
//
//	@doc:
//		Creates a parse handler for parsing a materialize operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMaterialize
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMaterialize(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphDynamicTableScan
//
//	@doc:
//		Creates a parse handler for parsing a dynamic table scan operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphDynamicTableScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerDynamicTableScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphDynamicIndexScan
//
//	@doc:
//		Creates a parse handler for parsing a dynamic index scan operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphDynamicIndexScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerDynamicIndexScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPartitionSelector
//
//	@doc:
//		Creates a parse handler for parsing a partition selector operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPartitionSelector
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPartitionSelector(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSequence
//
//	@doc:
//		Creates a parse handler for parsing a sequence operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSequence
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSequence(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLimit
//
//	@doc:
//		Creates a parse handler for parsing a Limit operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLimit
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLimit(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLimitcount
//
//	@doc:
//		Creates a parse handler for parsing a Limit Count operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLimitcount
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarLimitCount(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSubquery
//
//	@doc:
//		Creates a parse handler for parsing a scalar subquery operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSubquery
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubquery(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarBitmapBoolOp
//
//	@doc:
//		Creates a parse handler for parsing a scalar bitmap boolean operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarBitmapBoolOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarBitmapBoolOp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarArray
//
//	@doc:
//		Creates a parse handler for parsing a scalar array operator.
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarArray
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerArray(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarArrayRef
//
//	@doc:
//		Creates a parse handler for parsing a scalar arrayref operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarArrayRef
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarArrayRef(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarArrayRefIndexList
//
//	@doc:
//		Creates a parse handler for parsing an arrayref index list
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarArrayRefIndexList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarArrayRefIndexList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarAssertConstraintList
//
//	@doc:
//		Creates a parse handler for parsing a scalar assert predicate operator.
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarAssertConstraintList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarAssertConstraintList(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarDMLAction
//
//	@doc:
//		Creates a parse handler for parsing a scalar DML action operator.
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarDMLAction
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarDMLAction(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarOpList
//
//	@doc:
//		Creates a parse handler for parsing a scalar operator list
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarOpList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarOpList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarPartOid
//
//	@doc:
//		Creates a parse handler for parsing a scalar part OID
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarPartOid
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarPartOid(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarPartDefault
//
//	@doc:
//		Creates a parse handler for parsing a scalar part default
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarPartDefault
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarPartDefault(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarPartBound
//
//	@doc:
//		Creates a parse handler for parsing a scalar part boundary
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarPartBound
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarPartBound(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarPartBoundInclusion
//
//	@doc:
//		Creates a parse handler for parsing a scalar part bound inclusion
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarPartBoundInclusion
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarPartBoundInclusion(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarPartBoundOpen
//
//	@doc:
//		Creates a parse handler for parsing a scalar part bound openness
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarPartBoundOpen
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarPartBoundOpen(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphDirectDispatchInfo
//
//	@doc:
//		Creates a parse handler for parsing direct dispatch info
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphDirectDispatchInfo
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerDirectDispatchInfo(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLimitoffset
//
//	@doc:
//		Creates a parse handler for parsing a Limit Count operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLimitoffset
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarLimitOffset(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphGatherMotion
//
//	@doc:
//		Creates a parse handler for parsing a gather motion operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphGatherMotion
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerGatherMotion(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphBroadcastMotion
//
//	@doc:
//		Creates a parse handler for parsing a broadcast motion operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphBroadcastMotion
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerBroadcastMotion(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphRedistributeMotion
//
//	@doc:
//		Creates a parse handler for parsing a redistribute motion operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphRedistributeMotion
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerRedistributeMotion(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphRoutedMotion
//
//	@doc:
//		Creates a parse handler for parsing a routed motion operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphRoutedMotion
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerRoutedMotion(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphRandomMotion
//
//	@doc:
//		Creates a parse handler for parsing a random motion operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphRandomMotion
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerRandomMotion(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphAgg
//
//	@doc:
//		Creates a parse handler for parsing a group by operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphAgg
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerAgg(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphAggref
//
//	@doc:
//		Creates a parse handler for parsing aggref operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphAggref
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarAggref(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphGroupingColList
//
//	@doc:
//		Creates a parse handler for parsing a grouping cols list in a group by
//		operator
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphGroupingColList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerGroupingColList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarCmp
//
//	@doc:
//		Creates a parse handler for parsing a scalar comparison operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarCmp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarComp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphDistinctCmp
//
//	@doc:
//		Creates a parse handler for parsing a distinct comparison operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphDistinctCmp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerDistinctComp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarId
//
//	@doc:
//		Creates a parse handler for parsing a scalar identifier operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarId
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarIdent(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarFuncExpr
//
//	@doc:
//		Creates a parse handler for parsing a scalar FuncExpr
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarFuncExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarFuncExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarOpexpr
//
//	@doc:
//		Creates a parse handler for parsing a scalar OpExpr
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarOpexpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarOpExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarArrayCmp
//
//	@doc:
//		Creates a parse handler for parsing a scalar OpExpr
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarArrayCmp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarArrayComp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarBoolExpr
//
//	@doc:
//		Creates a parse handler for parsing a BoolExpr
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarBoolExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarBoolExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarMinMax
//
//	@doc:
//		Creates a parse handler for parsing a MinMax
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarMinMax
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarMinMax(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphBooleanTest
//
//	@doc:
//		Creates a parse handler for parsing a BooleanTest
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphBooleanTest
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarBooleanTest(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarNullTest
//
//	@doc:
//		Creates a parse handler for parsing a NullTest
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarNullTest
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarNullTest(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarNullIf
//
//	@doc:
//		Creates a parse handler for parsing a NullIf
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarNullIf
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarNullIf(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarCast
//
//	@doc:
//		Creates a parse handler for parsing a cast
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarCast
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarCast(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarCoerceToDomain
//
//	@doc:
//		Creates a parse handler for parsing a CoerceToDomain operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarCoerceToDomain
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarCoerceToDomain(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarCoerceViaIO
//
//	@doc:
//		Creates a parse handler for parsing a CoerceViaIO operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarCoerceViaIO
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarCoerceViaIO(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarArrayCoerceExpr
//
//	@doc:
//		Creates a parse handler for parsing an array coerce expression operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarArrayCoerceExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarArrayCoerceExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarInitPlan
//
//	@doc:
//		Creates a parse handler for parsing an InitPlan
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarInitPlan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarInitPlan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSubPlan
//
//	@doc:
//		Creates a parse handler for parsing a SubPlan.
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSubPlan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubPlan(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSubPlanTestExpr
//
//	@doc:
//		Creates a parse handler for parsing a SubPlan test expression
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSubPlanTestExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubPlanTestExpr(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSubPlanParamList
//
//	@doc:
//		Creates a parse handler for parsing a SubPlan Params DXL node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSubPlanParamList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubPlanParamList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSubPlanParam
//
//	@doc:
//		Creates a parse handler for parsing a single SubPlan Param
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSubPlanParam
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubPlanParam(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLogicalTVF
//
//	@doc:
//		Creates a parse handler for parsing a logical TVF
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLogicalTVF
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalTVF(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhysicalTVF
//
//	@doc:
//		Creates a parse handler for parsing a physical TVF
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhysicalTVF
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalTVF(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarCoalesce
//
//	@doc:
//		Creates a parse handler for parsing a coalesce operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarCoalesce
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarCoalesce(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSwitch
//
//	@doc:
//		Creates a parse handler for parsing a Switch operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSwitch
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSwitch(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarSwitchCase
//
//	@doc:
//		Creates a parse handler for parsing a SwitchCase operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarSwitchCase
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSwitchCase(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarCaseTest
//
//	@doc:
//		Creates a parse handler for parsing a case test
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarCaseTest
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarCaseTest(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarConstValue
//
//	@doc:
//		Creates a parse handler for parsing a Const
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarConstValue
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarConstValue(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphIfStmt
//
//	@doc:
//		Creates a parse handler for parsing an if statement
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphIfStmt
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarIfStmt(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphProjList
//
//	@doc:
//		Creates a parse handler for parsing a projection list
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphProjList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerProjList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphProjElem
//
//	@doc:
//		Creates a parse handler for parsing a projection element
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphProjElem
	(
		IMemoryPool *pmp,
		CParseHandlerManager *pphm,
		CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerProjElem(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphHashExprList
//
//	@doc:
//		Creates a parse handler for parsing a hash expr list
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphHashExprList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,	
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerHashExprList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphHashExpr
//
//	@doc:
//		Creates a parse handler for parsing a hash expression in a redistribute
//		motion node
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphHashExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerHashExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCondList
//
//	@doc:
//		Creates a parse handler for parsing a condition list in a hash join or
//		merge join node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCondList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCondList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSortColList
//
//	@doc:
//		Creates a parse handler for parsing a sorting column list in a sort node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSortColList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSortColList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSortCol
//
//	@doc:
//		Creates a parse handler for parsing a sorting column in a sort node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSortCol
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSortCol(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCost
//
//	@doc:
//		Creates a parse handler for parsing the cost estimates of a physical
//		operator
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCost
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCost(pmp, pphm, pphRoot);	
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphTableDesc
//
//	@doc:
//		Creates a parse handler for parsing a table descriptor
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphTableDesc
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerTableDescr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphColDesc
//
//	@doc:
//		Creates a parse handler for parsing a column descriptor
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphColDesc				
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerColDescr(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphIndexScan
//
//	@doc:
//		Creates a parse handler for parsing an index scan node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphIndexScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerIndexScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphIndexOnlyScan
//
//	@doc:
//		Creates a parse handler for parsing an index only scan node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphIndexOnlyScan
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerIndexOnlyScan(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphBitmapIndexProbe
//
//	@doc:
//		Creates a parse handler for parsing a bitmap index scan node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphBitmapIndexProbe
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarBitmapIndexProbe(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphIndexDescr
//
//	@doc:
//		Creates a parse handler for parsing an index descriptor of an
//		index scan node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphIndexDescr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerIndexDescr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphIndexCondList
//
//	@doc:
//		Creates a parse handler for parsing the list of index condition in a
//		index scan node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphIndexCondList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerIndexCondList(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphQuery
//
//	@doc:
//		Creates a parse handler for parsing a query
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphQuery
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerQuery(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgOp
//
//	@doc:
//		Creates a parse handler for parsing a logical operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalOp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgGet
//
//	@doc:
//		Creates a parse handler for parsing a logical get operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgGet
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalGet(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgExternalGet
//
//	@doc:
//		Creates a parse handler for parsing a logical external get operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgExternalGet
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalExternalGet(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgProject
//
//	@doc:
//		Creates a parse handler for parsing a logical project operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgProject
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalProject(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgCTEProducer
//
//	@doc:
//		Creates a parse handler for parsing a logical CTE producer operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgCTEProducer
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalCTEProducer(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgCTEConsumer
//
//	@doc:
//		Creates a parse handler for parsing a logical CTE consumer operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgCTEConsumer
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalCTEConsumer(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgCTEAnchor
//
//	@doc:
//		Creates a parse handler for parsing a logical CTE anchor operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgCTEAnchor
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalCTEAnchor(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCTEList
//
//	@doc:
//		Creates a parse handler for parsing a CTE list
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCTEList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCTEList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgSetOp
//
//	@doc:
//		Creates a parse handler for parsing a logical set operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgSetOp
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalSetOp(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgSelect
//
//	@doc:
//		Creates a parse handler for parsing a logical select operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgSelect
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalSelect(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgJoin
//
//	@doc:
//		Creates a parse handler for parsing a logical join operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgJoin
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalJoin(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphQueryOutput
//
//	@doc:
//		Creates a parse handler for parsing dxl representing query output
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphQueryOutput
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerQueryOutput(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgGrpBy
//
//	@doc:
//		Creates a parse handler for parsing a logical group by operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgGrpBy
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalGroupBy(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgLimit
//
//	@doc:
//		Creates a parse handler for parsing a logical limit operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgLimit
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalLimit(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgConstTable
//
//	@doc:
//		Creates a parse handler for parsing a logical constant table operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgConstTable
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalConstTable(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScSubqueryQuantified
//
//	@doc:
//		Creates a parse handler for parsing ALL/ANY subquery operators
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScSubqueryQuantified
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubqueryQuantified(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScSubqueryExists
//
//	@doc:
//		Creates a parse handler for parsing an EXISTS/NOT EXISTS subquery operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScSubqueryExists
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarSubqueryExists(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphStats
//
//	@doc:
//		Creates a parse handler for parsing relation statistics
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphStats
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerStatistics(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphStacktrace
//
//	@doc:
//		Creates a pass-through parse handler
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphStacktrace
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerStacktrace(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphStatsDerivedRelation
//
//	@doc:
//		Creates a parse handler for parsing relation statistics
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphStatsDerivedRelation
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerStatsDerivedRelation(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphStatsDerivedColumn
//
//	@doc:
//		Creates a parse handler for parsing derived column statistics
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphStatsDerivedColumn
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerStatsDerivedColumn(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphStatsBucketBound
//
//	@doc:
//		Creates a parse handler for parsing bucket bound in a histogram
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphStatsBucketBound
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerStatsBound(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindow
//
//	@doc:
//		Creates a parse handler for parsing a window node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindow
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalWindow(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowRef
//
//	@doc:
//		Creates a parse handler for parsing WindowRef operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowRef
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarWindowRef(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowFrame
//
//	@doc:
//		Creates a parse handler for parsing window frame node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowFrame
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerWindowFrame(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowKey
//
//	@doc:
//		Creates a parse handler for parsing window key node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowKey
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerWindowKey(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowKeyList
//
//	@doc:
//		Creates a parse handler for parsing a list of window keys
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowKeyList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerWindowKeyList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowSpec
//
//	@doc:
//		Creates a parse handler for parsing window specification node
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowSpec
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerWindowSpec(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowSpecList
//
//	@doc:
//		Creates a parse handler for parsing a list of window specifications
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowSpecList
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerWindowSpecList(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgWindow
//
//	@doc:
//		Creates a parse handler for parsing a logical window operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgWindow
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalWindow(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgInsert
//
//	@doc:
//		Creates a parse handler for parsing a logical insert operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgInsert
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalInsert(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgDelete
//
//	@doc:
//		Creates a parse handler for parsing a logical delete operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgDelete
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalDelete(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgUpdate
//
//	@doc:
//		Creates a parse handler for parsing a logical update operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgUpdate
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalUpdate(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphLgCTAS
//
//	@doc:
//		Creates a parse handler for parsing a logical CTAS operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphLgCTAS
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerLogicalCTAS(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhCTAS
//
//	@doc:
//		Creates a parse handler for parsing a physical CTAS operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhCTAS
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalCTAS(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCTASOptions
//
//	@doc:
//		Creates a parse handler for parsing CTAS storage options
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCTASOptions
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCtasStorageOptions(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhCTEProducer
//
//	@doc:
//		Creates a parse handler for parsing a physical CTE producer operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhCTEProducer
(
 IMemoryPool *pmp,
 CParseHandlerManager *pphm,
 CParseHandlerBase *pphRoot
 )
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalCTEProducer(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhCTEConsumer
//
//	@doc:
//		Creates a parse handler for parsing a physical CTE consumer operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhCTEConsumer
(
 IMemoryPool *pmp,
 CParseHandlerManager *pphm,
 CParseHandlerBase *pphRoot
 )
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalCTEConsumer(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhDML
//
//	@doc:
//		Creates a parse handler for parsing a physical DML operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhDML
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalDML(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhSplit
//
//	@doc:
//		Creates a parse handler for parsing a physical split operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhSplit
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalSplit(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhRowTrigger
//
//	@doc:
//		Creates a parse handler for parsing a physical row trigger operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhRowTrigger
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerPhysicalRowTrigger(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphPhAssert
//
//	@doc:
//		Creates a parse handler for parsing a physical assert operator
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphPhAssert
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerAssert(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowFrameTrailingEdge
//
//	@doc:
//		Creates a trailing window frame edge parser
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowFrameTrailingEdge
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarWindowFrameEdge(pmp, pphm, pphRoot, false /*fLeading*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphWindowFrameLeadingEdge
//
//	@doc:
//		Creates a leading window frame edge parser
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphWindowFrameLeadingEdge
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarWindowFrameEdge(pmp, pphm, pphRoot, true /*fLeading*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSearchStrategy
//
//	@doc:
//		Creates a parse handler for parsing search strategy
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSearchStrategy
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSearchStrategy(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphSearchStage
//
//	@doc:
//		Creates a parse handler for parsing search stage
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphSearchStage
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerSearchStage(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphXform
//
//	@doc:
//		Creates a parse handler for parsing xform
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphXform
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerXform(pmp, pphm, pphRoot);
}


//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCostParams
//
//	@doc:
//		Creates cost params parse handler
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCostParams
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCostParams(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphCostParam
//
//	@doc:
//		Creates cost param parse handler
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphCostParam
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerCostParam(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphScalarExpr
//
//	@doc:
//		Creates a parse handler for top level scalar expressions
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphScalarExpr
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerScalarExpr(pmp, pphm, pphRoot);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerFactory::PphMDGPDBCheckConstraint
//
//	@doc:
//		Creates a parse handler for parsing GPDB-specific check constraint
//
//---------------------------------------------------------------------------
CParseHandlerBase *
CParseHandlerFactory::PphMDGPDBCheckConstraint
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
{
	return GPOS_NEW(pmp) CParseHandlerMDGPDBCheckConstraint(pmp, pphm, pphRoot);
}

// EOF
