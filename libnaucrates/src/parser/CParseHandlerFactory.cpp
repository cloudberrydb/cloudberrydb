//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010-2011 EMC Corp.
//
//	@filename:
// CParseHandlerFactory.cpp
//
//	@doc:
// Implementation of the factory methods for creating parse handlers
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

CParseHandlerFactory::TokenParseHandlerFuncMap *
CParseHandlerFactory::m_token_parse_handler_func_map = NULL;

// adds a new mapping of token to corresponding parse handler
void
CParseHandlerFactory::AddMapping
	(
	Edxltoken token_type,
	ParseHandlerOpCreatorFunc *parse_handler_op_func
	)
{
	GPOS_ASSERT(NULL != m_token_parse_handler_func_map);
	const XMLCh *token_identifier_str = CDXLTokens::XmlstrToken(token_type);
	GPOS_ASSERT(NULL != token_identifier_str);
	
#ifdef GPOS_DEBUG
	BOOL success =
#endif
	m_token_parse_handler_func_map->Insert(token_identifier_str, parse_handler_op_func);
	
	GPOS_ASSERT(success);
}

// initialize mapping of tokens to parse handlers
void
CParseHandlerFactory::Init
	(
	IMemoryPool *mp
	)
{
	m_token_parse_handler_func_map = GPOS_NEW(mp) TokenParseHandlerFuncMap(mp, HASH_MAP_SIZE);
	
	// array mapping XML Token -> Parse Handler Creator mappings to hashmap
	SParseHandlerMapping token_parse_handler_map[] =
	{
			{EdxltokenPlan, &CreatePlanParseHandler},
			{EdxltokenMetadata, &CreateMetadataParseHandler},
			{EdxltokenMDRequest, &CreateMDRequestParseHandler},
			{EdxltokenTraceFlags, &CreateTraceFlagsParseHandler},
			{EdxltokenOptimizerConfig, &CreateOptimizerCfgParseHandler},
			{EdxltokenRelationExternal, &CreateMDRelationExtParseHandler},
			{EdxltokenRelationCTAS, &CreateMDRelationCTASParseHandler},
			{EdxltokenEnumeratorConfig, &CreateEnumeratorCfgParseHandler},
			{EdxltokenStatisticsConfig, &CreateStatisticsCfgParseHandler},
			{EdxltokenCTEConfig, &CreateCTECfgParseHandler},
			{EdxltokenCostModelConfig, &CreateCostModelCfgParseHandler},
			{EdxltokenHint, &CreateHintParseHandler},
			{EdxltokenWindowOids, &CreateWindowOidsParseHandler},

			{EdxltokenRelation, &CreateMDRelationParseHandler},
			{EdxltokenIndex, &CreateMDIndexParseHandler},
			{EdxltokenMDType, &CreateMDTypeParseHandler},
			{EdxltokenGPDBScalarOp, &CreateMDScalarOpParseHandler},
			{EdxltokenGPDBFunc, &CreateMDFuncParseHandler},
			{EdxltokenGPDBAgg, &CreateMDAggParseHandler},
			{EdxltokenGPDBTrigger, &CreateMDTriggerParseHandler},
			{EdxltokenCheckConstraint, &CreateMDChkConstraintParseHandler},
			{EdxltokenRelationStats, &CreateRelStatsParseHandler},
			{EdxltokenColumnStats, &CreateColStatsParseHandler},
			{EdxltokenMetadataIdList, &CreateMDIdListParseHandler},
			{EdxltokenIndexInfoList, &CreateMDIndexInfoListParseHandler},
			{EdxltokenMetadataColumns, &CreateMDColsParseHandler},
			{EdxltokenMetadataColumn, &CreateMDColParseHandler},
			{EdxltokenColumnDefaultValue, &CreateColDefaultValExprParseHandler},
			{EdxltokenColumnStatsBucket, &CreateColStatsBucketParseHandler},
			{EdxltokenGPDBCast, &CreateMDCastParseHandler},
			{EdxltokenGPDBMDScCmp, &CreateMDScCmpParseHandler},
			{EdxltokenGPDBArrayCoerceCast, &CreateMDArrayCoerceCastParseHandler},

			{EdxltokenPhysical, &CreatePhysicalOpParseHandler},

			{EdxltokenPhysicalAggregate, &CreateAggParseHandler},
			{EdxltokenPhysicalTableScan, &CreateTableScanParseHandler},
			{EdxltokenPhysicalBitmapTableScan, &CreateBitmapTableScanParseHandler},
			{EdxltokenPhysicalDynamicBitmapTableScan, &CreateDynBitmapTableScanParseHandler},
			{EdxltokenPhysicalExternalScan, &CreateExternalScanParseHandler},
			{EdxltokenPhysicalHashJoin, &CreateHashJoinParseHandler},
			{EdxltokenPhysicalNLJoin, &CreateNLJoinParseHandler},
			{EdxltokenPhysicalMergeJoin, &CreateMergeJoinParseHandler},
			{EdxltokenPhysicalGatherMotion, &CreateGatherMotionParseHandler},
			{EdxltokenPhysicalBroadcastMotion, &CreateBroadcastMotionParseHandler},
			{EdxltokenPhysicalRedistributeMotion, &CreateRedistributeMotionParseHandler},
			{EdxltokenPhysicalRoutedDistributeMotion, &CreateRoutedMotionParseHandler},
			{EdxltokenPhysicalRandomMotion, &CreateRandomMotionParseHandler},
			{EdxltokenPhysicalSubqueryScan, &CreateSubqueryScanParseHandler},
			{EdxltokenPhysicalResult, &CreateResultParseHandler},
			{EdxltokenPhysicalLimit, &CreateLimitParseHandler},
			{EdxltokenPhysicalSort, &CreateSortParseHandler},
			{EdxltokenPhysicalAppend, &CreateAppendParseHandler},
			{EdxltokenPhysicalMaterialize, &CreateMaterializeParseHandler},
		 	{EdxltokenPhysicalDynamicTableScan, &CreateDTSParseHandler},
		 	{EdxltokenPhysicalDynamicIndexScan, &CreateDynamicIdxScanParseHandler},
		 	{EdxltokenPhysicalPartitionSelector, &CreatePartitionSelectorParseHandler},
			{EdxltokenPhysicalSequence, &CreateSequenceParseHandler},
			{EdxltokenPhysicalIndexScan, &CreateIdxScanListParseHandler},
			{EdxltokenPhysicalIndexOnlyScan, &CreateIdxOnlyScanParseHandler},
			{EdxltokenScalarBitmapIndexProbe, &CreateBitmapIdxProbeParseHandler},
			{EdxltokenIndexDescr, &CreateIdxDescrParseHandler},

			{EdxltokenPhysicalWindow, &CreateWindowParseHandler},
			{EdxltokenScalarWindowref, &CreateWindowRefParseHandler},
			{EdxltokenWindowFrame, &CreateWindowFrameParseHandler},
			{EdxltokenScalarWindowFrameLeadingEdge, &CreateFrameLeadingEdgeParseHandler},
			{EdxltokenScalarWindowFrameTrailingEdge, &CreateFrameTrailingEdgeParseHandler},
			{EdxltokenWindowKey, &CreateWindowKeyParseHandler},
			{EdxltokenWindowKeyList, &CreateWindowKeyListParseHandler},

			{EdxltokenScalarIndexCondList, &CreateIdxCondListParseHandler},
 
			{EdxltokenScalar, &CreateScalarOpParseHandler},

			{EdxltokenScalarFilter, &CreateFilterParseHandler},
			{EdxltokenScalarOneTimeFilter, &CreateFilterParseHandler},
			{EdxltokenScalarRecheckCondFilter, &CreateFilterParseHandler},
			{EdxltokenScalarProjList, &CreateProjListParseHandler},
			{EdxltokenScalarProjElem, &CreateProjElemParseHandler},
			{EdxltokenScalarAggref, &CreateAggRefParseHandler},
			{EdxltokenScalarSortColList, &CreateSortColListParseHandler},
			{EdxltokenScalarSortCol, &CreateSortColParseHandler},
			{EdxltokenScalarCoalesce, &CreateScCoalesceParseHandler},
			{EdxltokenScalarComp, &CreateScCmpParseHandler},
			{EdxltokenScalarDistinctComp, &CreateDistinctCmpParseHandler},
			{EdxltokenScalarIdent, &CreateScIdParseHandler},
			{EdxltokenScalarOpExpr, &CreateScOpExprParseHandler},
			{EdxltokenScalarArrayComp, &CreateScArrayCmpParseHandler},
			{EdxltokenScalarBoolOr, &CreateScBoolExprParseHandler},
			{EdxltokenScalarBoolNot, &CreateScBoolExprParseHandler},
			{EdxltokenScalarBoolAnd, &CreateScBoolExprParseHandler},
			{EdxltokenScalarMin, &CreateScMinMaxParseHandler},
			{EdxltokenScalarMax, &CreateScMinMaxParseHandler},
			{EdxltokenScalarBoolTestIsTrue, &CreateBooleanTestParseHandler},
			{EdxltokenScalarBoolTestIsNotTrue, &CreateBooleanTestParseHandler},
			{EdxltokenScalarBoolTestIsFalse, &CreateBooleanTestParseHandler},
			{EdxltokenScalarBoolTestIsNotFalse, &CreateBooleanTestParseHandler},
			{EdxltokenScalarBoolTestIsUnknown, &CreateBooleanTestParseHandler},
			{EdxltokenScalarBoolTestIsNotUnknown, &CreateBooleanTestParseHandler},
			{EdxltokenScalarSubPlan, &CreateScSubPlanParseHandler},
			{EdxltokenScalarConstValue, &CreateScConstValueParseHandler},
			{EdxltokenScalarIfStmt, &CreateIfStmtParseHandler},
			{EdxltokenScalarSwitch, &CreateScSwitchParseHandler},
			{EdxltokenScalarSwitchCase, &CreateScSwitchCaseParseHandler},
			{EdxltokenScalarCaseTest, &CreateScCaseTestParseHandler},
			{EdxltokenScalarFuncExpr, &CreateScFuncExprParseHandler},
			{EdxltokenScalarIsNull, &CreateScNullTestParseHandler},
			{EdxltokenScalarIsNotNull, &CreateScNullTestParseHandler},
			{EdxltokenScalarNullIf, &CreateScNullIfParseHandler},
			{EdxltokenScalarCast, &CreateScCastParseHandler},
			{EdxltokenScalarCoerceToDomain, CreateScCoerceToDomainParseHandler},
			{EdxltokenScalarCoerceViaIO, CreateScCoerceViaIOParseHandler},
			{EdxltokenScalarArrayCoerceExpr, CreateScArrayCoerceExprParseHandler},
			{EdxltokenScalarHashExpr, &CreateHashExprParseHandler},
			{EdxltokenScalarHashCondList, &CreateCondListParseHandler},
			{EdxltokenScalarMergeCondList, &CreateCondListParseHandler},
			{EdxltokenScalarHashExprList, &CreateHashExprListParseHandler},
			{EdxltokenScalarGroupingColList, &CreateGroupingColListParseHandler},
			{EdxltokenScalarLimitOffset, &CreateLimitOffsetParseHandler},
			{EdxltokenScalarLimitCount, &CreateLimitCountParseHandler},
			{EdxltokenScalarSubPlanTestExpr, &CreateScSubPlanTestExprParseHandler},
			{EdxltokenScalarSubPlanParamList, &CreateScSubPlanParamListParseHandler},
			{EdxltokenScalarSubPlanParam, &CreateScSubPlanParamParseHandler},
			{EdxltokenScalarOpList, &CreateScOpListParseHandler},
			{EdxltokenScalarPartOid, &CreateScPartOidParseHandler},
			{EdxltokenScalarPartDefault, &CreateScPartDefaultParseHandler},
			{EdxltokenScalarPartBound, &CreateScPartBoundParseHandler},
			{EdxltokenScalarPartBoundInclusion, &CreateScPartBoundInclParseHandler},
			{EdxltokenScalarPartBoundOpen, &CreateScPartBoundOpenParseHandler},
			{EdxltokenScalarPartListValues, &CreateScPartListValuesParseHandler},
			{EdxltokenScalarPartListNullTest, &CreateScPartListNullTestParseHandler},

			{EdxltokenScalarSubquery, &CreateScSubqueryParseHandler},
			{EdxltokenScalarBitmapAnd, &CreateScBitmapBoolOpParseHandler},
			{EdxltokenScalarBitmapOr, &CreateScBitmapBoolOpParseHandler},

			{EdxltokenScalarArray, &CreateScArrayParseHandler},
			{EdxltokenScalarArrayRef, &CreateScArrayRefParseHandler},
			{EdxltokenScalarArrayRefIndexList, &CreateScArrayRefIdxListParseHandler},
			
			{EdxltokenScalarAssertConstraintList, &CreateScAssertConstraintListParseHandler},
			
			{EdxltokenScalarDMLAction, &CreateScDMLActionParseHandler},
			{EdxltokenDirectDispatchInfo, &CreateDirectDispatchParseHandler},

			{EdxltokenQueryOutput, &CreateLogicalQueryOpParseHandler},

			{EdxltokenCost, &CreateCostParseHandler},
			{EdxltokenTableDescr, &CreateTableDescParseHandler},
			{EdxltokenColumns, &CreateColDescParseHandler},
			{EdxltokenProperties, &CreatePropertiesParseHandler},
			{EdxltokenPhysicalTVF, &CreatePhysicalTVFParseHandler},
			{EdxltokenLogicalTVF, &CreateLogicalTVFParseHandler},

			{EdxltokenQuery, &CreateQueryParseHandler},
			{EdxltokenLogicalGet, &CreateLogicalGetParseHandler},
			{EdxltokenLogicalExternalGet, &CreateLogicalExtGetParseHandler},
			{EdxltokenLogical, &CreateLogicalOpParseHandler},
			{EdxltokenLogicalProject, &CreateLogicalProjParseHandler},
			{EdxltokenLogicalSelect, &CreateLogicalSelectParseHandler},
			{EdxltokenLogicalJoin, &CreateLogicalJoinParseHandler},
			{EdxltokenLogicalGrpBy, &CreateLogicalGrpByParseHandler},
			{EdxltokenLogicalLimit, &CreateLogicalLimitParseHandler},
			{EdxltokenLogicalConstTable, &CreateLogicalConstTableParseHandler},
			{EdxltokenLogicalCTEProducer, &CreateLogicalCTEProdParseHandler},
			{EdxltokenLogicalCTEConsumer, &CreateLogicalCTEConsParseHandler},
			{EdxltokenLogicalCTEAnchor, &CreateLogicalCTEAnchorParseHandler},
			{EdxltokenCTEList, &CreateCTEListParseHandler},

			{EdxltokenLogicalWindow, &CreateLogicalWindowParseHandler},
			{EdxltokenWindowSpec, &CreateWindowSpecParseHandler},
			{EdxltokenWindowSpecList, &CreateWindowSpecListParseHandler},

			{EdxltokenLogicalInsert, &CreateLogicalInsertParseHandler},
			{EdxltokenLogicalDelete, &CreateLogicalDeleteParseHandler},
			{EdxltokenLogicalUpdate, &CreateLogicalUpdateParseHandler},
			{EdxltokenPhysicalDMLInsert, &CreatePhysicalDMLParseHandler},
			{EdxltokenPhysicalDMLDelete, &CreatePhysicalDMLParseHandler},
			{EdxltokenPhysicalDMLUpdate, &CreatePhysicalDMLParseHandler},
			{EdxltokenPhysicalSplit, &CreatePhysicalSplitParseHandler},
			{EdxltokenPhysicalRowTrigger, &CreatePhysicalRowTriggerParseHandler},
			{EdxltokenPhysicalAssert, &CreatePhysicalAssertParseHandler},
			{EdxltokenPhysicalCTEProducer, &CreatePhysicalCTEProdParseHandler},
			{EdxltokenPhysicalCTEConsumer, &CreatePhysicalCTEConsParseHandler},
			{EdxltokenLogicalCTAS, &CreateLogicalCTASParseHandler},
			{EdxltokenPhysicalCTAS, &CreatePhysicalCTASParseHandler},
			{EdxltokenCTASOptions, &CreateCTASOptionsParseHandler},
		
			{EdxltokenScalarSubqueryAny, &CreateScScalarSubqueryQuantifiedParseHandler},
			{EdxltokenScalarSubqueryAll, &CreateScScalarSubqueryQuantifiedParseHandler},
 			{EdxltokenScalarSubqueryExists, &CreateScScalarSubqueryExistsParseHandler},
			{EdxltokenScalarSubqueryNotExists, &CreateScScalarSubqueryExistsParseHandler},

			{EdxltokenStackTrace, &CreateStackTraceParseHandler},
			{EdxltokenLogicalUnion, &CreateLogicalSetOpParseHandler},
			{EdxltokenLogicalUnionAll, &CreateLogicalSetOpParseHandler},
			{EdxltokenLogicalIntersect, &CreateLogicalSetOpParseHandler},
			{EdxltokenLogicalIntersectAll, &CreateLogicalSetOpParseHandler},
			{EdxltokenLogicalDifference, &CreateLogicalSetOpParseHandler},
			{EdxltokenLogicalDifferenceAll, &CreateLogicalSetOpParseHandler},

			{EdxltokenStatistics, &CreateStatsParseHandler},
			{EdxltokenStatsDerivedColumn, &CreateStatsDrvdColParseHandler},
			{EdxltokenStatsDerivedRelation, &CreateStatsDrvdRelParseHandler},
			{EdxltokenStatsBucketLowerBound, &CreateStatsBucketBoundParseHandler},
			{EdxltokenStatsBucketUpperBound, &CreateStatsBucketBoundParseHandler},

			{EdxltokenSearchStrategy, &CreateSearchStrategyParseHandler},
			{EdxltokenSearchStage, &CreateSearchStageParseHandler},
			{EdxltokenXform, &CreateXformParseHandler},

			{EdxltokenCostParams, &CreateCostParamsParseHandler},
			{EdxltokenCostParam, &CreateCostParamParseHandler},

			{EdxltokenScalarExpr, &CreateScExprParseHandler},
			{EdxltokenScalarValuesList, &CreateScValuesListParseHandler},
			{EdxltokenPhysicalValuesScan, &CreateValuesScanParseHandler},
			{EdxltokenNLJIndexParamList, &CreateNLJIndexParamListParseHandler},
			{EdxltokenNLJIndexParam, &CreateNLJIndexParamParseHandler}

	};
	
	const ULONG num_of_parse_handlers = GPOS_ARRAY_SIZE(token_parse_handler_map);

	for (ULONG idx = 0; idx < num_of_parse_handlers; idx++)
	{
		SParseHandlerMapping elem = token_parse_handler_map[idx];
		AddMapping(elem.token_type, elem.parse_handler_op_func);
	}
}

// creates a parse handler instance given an xml tag
CParseHandlerBase *
CParseHandlerFactory::GetParseHandler
	(
	IMemoryPool *mp,
	const XMLCh *token_identifier_str,
	CParseHandlerManager* parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	GPOS_ASSERT(NULL != m_token_parse_handler_func_map);

	ParseHandlerOpCreatorFunc *create_parse_handler_func = m_token_parse_handler_func_map->Find(token_identifier_str);

	if (create_parse_handler_func != NULL)
	{
		return (*create_parse_handler_func) (mp, parse_handler_mgr, parse_handler_root);
	}
	
	CDXLMemoryManager dxl_memory_manager(mp);

	// did not find the physical operator in the table
	CWStringDynamic *str = CDXLUtils::CreateDynamicStringFromXMLChArray(&dxl_memory_manager, token_identifier_str);;

	GPOS_RAISE
	(
	gpdxl::ExmaDXL,
	gpdxl::ExmiDXLUnrecognizedOperator,
	str->GetBuffer()
	);

	return NULL;
}

// creates a parse handler for parsing a DXL document.
CParseHandlerDXL *
CParseHandlerFactory::GetParseHandlerDXL
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr
	)
{
	return GPOS_NEW(mp) CParseHandlerDXL(mp, parse_handler_mgr);
}

// creates a parse handler for parsing a Plan
CParseHandlerBase *
CParseHandlerFactory::CreatePlanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPlan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMetadataParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMetadata(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a metadata request
CParseHandlerBase *
CParseHandlerFactory::CreateMDRequestParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDRequest(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing trace flags
CParseHandlerBase *
CParseHandlerFactory::CreateTraceFlagsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerTraceFlags(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing optimizer config
CParseHandlerBase *
CParseHandlerFactory::CreateOptimizerCfgParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerOptimizerConfig(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing enumerator config
CParseHandlerBase *
CParseHandlerFactory::CreateEnumeratorCfgParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerEnumeratorConfig(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing statistics configuration
CParseHandlerBase *
CParseHandlerFactory::CreateStatisticsCfgParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerStatisticsConfig(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing CTE configuration
CParseHandlerBase *
CParseHandlerFactory::CreateCTECfgParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCTEConfig(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing cost model configuration
CParseHandlerBase *
CParseHandlerFactory::CreateCostModelCfgParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCostModel(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing hint configuration
CParseHandlerBase *
CParseHandlerFactory::CreateHintParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerHint(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing window oids configuration
CParseHandlerBase *
CParseHandlerFactory::CreateWindowOidsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerWindowOids(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing relation metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDRelationParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDRelation(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing external relation metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDRelationExtParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDRelationExternal(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing CTAS relation metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDRelationCTASParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDRelationCtas(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a MD index
CParseHandlerBase *
CParseHandlerFactory::CreateMDIndexParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDIndex(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing relation stats
CParseHandlerBase *
CParseHandlerFactory::CreateRelStatsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerRelStats(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing column stats
CParseHandlerBase *
CParseHandlerFactory::CreateColStatsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerColStats(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing column stats bucket
CParseHandlerBase *
CParseHandlerFactory::CreateColStatsBucketParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerColStatsBucket(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB type metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDTypeParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDType(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific operator metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDScalarOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDGPDBScalarOp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific function metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDFuncParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDGPDBFunc(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific aggregate metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDAggParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDGPDBAgg(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific trigger metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDTriggerParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDGPDBTrigger(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific cast metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDCastParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDCast(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific scalar comparison metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDScCmpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDScCmp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a list of metadata identifiers
CParseHandlerBase *
CParseHandlerFactory::CreateMDIdListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMetadataIdList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a list of column metadata info
CParseHandlerBase *
CParseHandlerFactory::CreateMDColsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMetadataColumns(mp, parse_handler_mgr, parse_handler_root);
}

CParseHandlerBase *
CParseHandlerFactory::CreateMDIndexInfoListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDIndexInfoList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing column info
CParseHandlerBase *
CParseHandlerFactory::CreateMDColParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMetadataColumn(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a a default value for a column
CParseHandlerBase *
CParseHandlerFactory::CreateColDefaultValExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerDefaultValueExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalOp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar operator
CParseHandlerBase *
CParseHandlerFactory::CreateScalarOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarOp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing the properties of a physical operator
CParseHandlerBase *
CParseHandlerFactory::CreatePropertiesParseHandler
	(
		IMemoryPool *mp,
		CParseHandlerManager *parse_handler_mgr,
		CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerProperties(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a filter operator
CParseHandlerBase *
CParseHandlerFactory::CreateFilterParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerFilter(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a table scan
CParseHandlerBase *
CParseHandlerFactory::CreateTableScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerTableScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a bitmap table scan
CParseHandlerBase *
CParseHandlerFactory::CreateBitmapTableScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalBitmapTableScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a dynamic bitmap table scan
CParseHandlerBase *
CParseHandlerFactory::CreateDynBitmapTableScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalDynamicBitmapTableScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an external scan
CParseHandlerBase *
CParseHandlerFactory::CreateExternalScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerExternalScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a subquery scan
CParseHandlerBase *
CParseHandlerFactory::CreateSubqueryScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSubqueryScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a result node
CParseHandlerBase *
CParseHandlerFactory::CreateResultParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerResult(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a hash join operator
CParseHandlerBase *
CParseHandlerFactory::CreateHashJoinParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerHashJoin(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a nested loop join operator
CParseHandlerBase *
CParseHandlerFactory::CreateNLJoinParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerNLJoin(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a merge join operator
CParseHandlerBase *
CParseHandlerFactory::CreateMergeJoinParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMergeJoin(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a sort operator
CParseHandlerBase *
CParseHandlerFactory::CreateSortParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSort(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an append operator
CParseHandlerBase *
CParseHandlerFactory::CreateAppendParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerAppend(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a materialize operator
CParseHandlerBase *
CParseHandlerFactory::CreateMaterializeParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMaterialize(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a dynamic table scan operator
CParseHandlerBase *
CParseHandlerFactory::CreateDTSParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerDynamicTableScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a dynamic index scan operator
CParseHandlerBase *
CParseHandlerFactory::CreateDynamicIdxScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerDynamicIndexScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a partition selector operator
CParseHandlerBase *
CParseHandlerFactory::CreatePartitionSelectorParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPartitionSelector(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a sequence operator
CParseHandlerBase *
CParseHandlerFactory::CreateSequenceParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSequence(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Limit operator
CParseHandlerBase *
CParseHandlerFactory::CreateLimitParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLimit(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Limit Count operator
CParseHandlerBase *
CParseHandlerFactory::CreateLimitCountParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarLimitCount(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar subquery operator
CParseHandlerBase *
CParseHandlerFactory::CreateScSubqueryParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubquery(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar bitmap boolean operator
CParseHandlerBase *
CParseHandlerFactory::CreateScBitmapBoolOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarBitmapBoolOp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar array operator.
CParseHandlerBase *
CParseHandlerFactory::CreateScArrayParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerArray(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar arrayref operator
CParseHandlerBase *
CParseHandlerFactory::CreateScArrayRefParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarArrayRef(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an arrayref index list
CParseHandlerBase *
CParseHandlerFactory::CreateScArrayRefIdxListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarArrayRefIndexList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar assert predicate operator.
CParseHandlerBase *
CParseHandlerFactory::CreateScAssertConstraintListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarAssertConstraintList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar DML action operator.
CParseHandlerBase *
CParseHandlerFactory::CreateScDMLActionParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarDMLAction(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar operator list
CParseHandlerBase *
CParseHandlerFactory::CreateScOpListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarOpList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part OID
CParseHandlerBase *
CParseHandlerFactory::CreateScPartOidParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartOid(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part default
CParseHandlerBase *
CParseHandlerFactory::CreateScPartDefaultParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartDefault(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part boundary
CParseHandlerBase *
CParseHandlerFactory::CreateScPartBoundParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartBound(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part bound inclusion
CParseHandlerBase *
CParseHandlerFactory::CreateScPartBoundInclParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartBoundInclusion(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part bound openness
CParseHandlerBase *
CParseHandlerFactory::CreateScPartBoundOpenParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartBoundOpen(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part list values
CParseHandlerBase *
CParseHandlerFactory::CreateScPartListValuesParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartListValues(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar part list null test
CParseHandlerBase *
CParseHandlerFactory::CreateScPartListNullTestParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarPartListNullTest(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing direct dispatch info
CParseHandlerBase *
CParseHandlerFactory::CreateDirectDispatchParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerDirectDispatchInfo(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Limit Count operator
CParseHandlerBase *
CParseHandlerFactory::CreateLimitOffsetParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarLimitOffset(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a gather motion operator
CParseHandlerBase *
CParseHandlerFactory::CreateGatherMotionParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerGatherMotion(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a broadcast motion operator
CParseHandlerBase *
CParseHandlerFactory::CreateBroadcastMotionParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerBroadcastMotion(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a redistribute motion operator
CParseHandlerBase *
CParseHandlerFactory::CreateRedistributeMotionParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerRedistributeMotion(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a routed motion operator
CParseHandlerBase *
CParseHandlerFactory::CreateRoutedMotionParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerRoutedMotion(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a random motion operator
CParseHandlerBase *
CParseHandlerFactory::CreateRandomMotionParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerRandomMotion(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a group by operator
CParseHandlerBase *
CParseHandlerFactory::CreateAggParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerAgg(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing aggref operator
CParseHandlerBase *
CParseHandlerFactory::CreateAggRefParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarAggref(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a grouping cols list in a group by
// operator
CParseHandlerBase *
CParseHandlerFactory::CreateGroupingColListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerGroupingColList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar comparison operator
CParseHandlerBase *
CParseHandlerFactory::CreateScCmpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarComp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a distinct comparison operator
CParseHandlerBase *
CParseHandlerFactory::CreateDistinctCmpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerDistinctComp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar identifier operator
CParseHandlerBase *
CParseHandlerFactory::CreateScIdParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarIdent(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar FuncExpr
CParseHandlerBase *
CParseHandlerFactory::CreateScFuncExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarFuncExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar OpExpr
CParseHandlerBase *
CParseHandlerFactory::CreateScOpExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarOpExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a scalar OpExpr
CParseHandlerBase *
CParseHandlerFactory::CreateScArrayCmpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarArrayComp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a BoolExpr
CParseHandlerBase *
CParseHandlerFactory::CreateScBoolExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarBoolExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a MinMax
CParseHandlerBase *
CParseHandlerFactory::CreateScMinMaxParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarMinMax(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a BooleanTest
CParseHandlerBase *
CParseHandlerFactory::CreateBooleanTestParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarBooleanTest(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a NullTest
CParseHandlerBase *
CParseHandlerFactory::CreateScNullTestParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarNullTest(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a NullIf
CParseHandlerBase *
CParseHandlerFactory::CreateScNullIfParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarNullIf(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a cast
CParseHandlerBase *
CParseHandlerFactory::CreateScCastParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarCast(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a CoerceToDomain operator
CParseHandlerBase *
CParseHandlerFactory::CreateScCoerceToDomainParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarCoerceToDomain(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a CoerceViaIO operator
CParseHandlerBase *
CParseHandlerFactory::CreateScCoerceViaIOParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarCoerceViaIO(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an array coerce expression operator
CParseHandlerBase *
CParseHandlerFactory::CreateScArrayCoerceExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarArrayCoerceExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a SubPlan.
CParseHandlerBase *
CParseHandlerFactory::CreateScSubPlanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubPlan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a SubPlan test expression
CParseHandlerBase *
CParseHandlerFactory::CreateScSubPlanTestExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubPlanTestExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a SubPlan Params DXL node
CParseHandlerBase *
CParseHandlerFactory::CreateScSubPlanParamListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubPlanParamList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a single SubPlan Param
CParseHandlerBase *
CParseHandlerFactory::CreateScSubPlanParamParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubPlanParam(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical TVF
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalTVFParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalTVF(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical TVF
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalTVFParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalTVF(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a coalesce operator
CParseHandlerBase *
CParseHandlerFactory::CreateScCoalesceParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarCoalesce(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Switch operator
CParseHandlerBase *
CParseHandlerFactory::CreateScSwitchParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSwitch(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a SwitchCase operator
CParseHandlerBase *
CParseHandlerFactory::CreateScSwitchCaseParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSwitchCase(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a case test
CParseHandlerBase *
CParseHandlerFactory::CreateScCaseTestParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarCaseTest(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Const
CParseHandlerBase *
CParseHandlerFactory::CreateScConstValueParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarConstValue(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an if statement
CParseHandlerBase *
CParseHandlerFactory::CreateIfStmtParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarIfStmt(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a projection list
CParseHandlerBase *
CParseHandlerFactory::CreateProjListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerProjList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a projection element
CParseHandlerBase *
CParseHandlerFactory::CreateProjElemParseHandler
	(
		IMemoryPool *mp,
		CParseHandlerManager *parse_handler_mgr,
		CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerProjElem(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a hash expr list
CParseHandlerBase *
CParseHandlerFactory::CreateHashExprListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,	
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerHashExprList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a hash expression in a redistribute
// motion node
CParseHandlerBase *
CParseHandlerFactory::CreateHashExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerHashExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a condition list in a hash join or
// merge join node
CParseHandlerBase *
CParseHandlerFactory::CreateCondListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCondList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a sorting column list in a sort node
CParseHandlerBase *
CParseHandlerFactory::CreateSortColListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSortColList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a sorting column in a sort node
CParseHandlerBase *
CParseHandlerFactory::CreateSortColParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSortCol(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing the cost estimates of a physical
// operator
CParseHandlerBase *
CParseHandlerFactory::CreateCostParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCost(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a table descriptor
CParseHandlerBase *
CParseHandlerFactory::CreateTableDescParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerTableDescr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a column descriptor
CParseHandlerBase *
CParseHandlerFactory::CreateColDescParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerColDescr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an index scan node
CParseHandlerBase *
CParseHandlerFactory::CreateIdxScanListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerIndexScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an index only scan node
CParseHandlerBase *
CParseHandlerFactory::CreateIdxOnlyScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerIndexOnlyScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a bitmap index scan node
CParseHandlerBase *
CParseHandlerFactory::CreateBitmapIdxProbeParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarBitmapIndexProbe(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an index descriptor of an
// index scan node
CParseHandlerBase *
CParseHandlerFactory::CreateIdxDescrParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerIndexDescr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing the list of index condition in a
// index scan node
CParseHandlerBase *
CParseHandlerFactory::CreateIdxCondListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerIndexCondList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a query
CParseHandlerBase *
CParseHandlerFactory::CreateQueryParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerQuery(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalOp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical get operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalGetParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalGet(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical external get operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalExtGetParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalExternalGet(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical project operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalProjParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalProject(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical CTE producer operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalCTEProdParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalCTEProducer(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical CTE consumer operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalCTEConsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalCTEConsumer(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical CTE anchor operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalCTEAnchorParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalCTEAnchor(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a CTE list
CParseHandlerBase *
CParseHandlerFactory::CreateCTEListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCTEList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical set operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalSetOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalSetOp(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical select operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalSelectParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalSelect(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical join operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalJoinParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalJoin(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing dxl representing query output
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalQueryOpParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerQueryOutput(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical group by operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalGrpByParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalGroupBy(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical limit operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalLimitParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalLimit(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical constant table operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalConstTableParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalConstTable(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing ALL/ANY subquery operators
CParseHandlerBase *
CParseHandlerFactory::CreateScScalarSubqueryQuantifiedParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubqueryQuantified(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing an EXISTS/NOT EXISTS subquery operator
CParseHandlerBase *
CParseHandlerFactory::CreateScScalarSubqueryExistsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarSubqueryExists(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing relation statistics
CParseHandlerBase *
CParseHandlerFactory::CreateStatsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerStatistics(mp, parse_handler_mgr, parse_handler_root);
}

// creates a pass-through parse handler
CParseHandlerBase *
CParseHandlerFactory::CreateStackTraceParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerStacktrace(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing relation statistics
CParseHandlerBase *
CParseHandlerFactory::CreateStatsDrvdRelParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerStatsDerivedRelation(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing derived column statistics
CParseHandlerBase *
CParseHandlerFactory::CreateStatsDrvdColParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerStatsDerivedColumn(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing bucket bound in a histogram
CParseHandlerBase *
CParseHandlerFactory::CreateStatsBucketBoundParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerStatsBound(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a window node
CParseHandlerBase *
CParseHandlerFactory::CreateWindowParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalWindow(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing WindowRef operator
CParseHandlerBase *
CParseHandlerFactory::CreateWindowRefParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarWindowRef(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing window frame node
CParseHandlerBase *
CParseHandlerFactory::CreateWindowFrameParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerWindowFrame(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing window key node
CParseHandlerBase *
CParseHandlerFactory::CreateWindowKeyParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerWindowKey(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a list of window keys
CParseHandlerBase *
CParseHandlerFactory::CreateWindowKeyListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerWindowKeyList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing window specification node
CParseHandlerBase *
CParseHandlerFactory::CreateWindowSpecParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerWindowSpec(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a list of window specifications
CParseHandlerBase *
CParseHandlerFactory::CreateWindowSpecListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerWindowSpecList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical window operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalWindowParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalWindow(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical insert operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalInsertParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalInsert(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical delete operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalDeleteParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalDelete(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical update operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalUpdateParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalUpdate(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a logical CTAS operator
CParseHandlerBase *
CParseHandlerFactory::CreateLogicalCTASParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerLogicalCTAS(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical CTAS operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalCTASParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalCTAS(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing CTAS storage options
CParseHandlerBase *
CParseHandlerFactory::CreateCTASOptionsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCtasStorageOptions(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical CTE producer operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalCTEProdParseHandler
(
 IMemoryPool *mp,
 CParseHandlerManager *parse_handler_mgr,
 CParseHandlerBase *parse_handler_root
 )
{
	return GPOS_NEW(mp) CParseHandlerPhysicalCTEProducer(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical CTE consumer operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalCTEConsParseHandler
(
 IMemoryPool *mp,
 CParseHandlerManager *parse_handler_mgr,
 CParseHandlerBase *parse_handler_root
 )
{
	return GPOS_NEW(mp) CParseHandlerPhysicalCTEConsumer(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical DML operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalDMLParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalDML(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical split operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalSplitParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalSplit(mp, parse_handler_mgr, parse_handler_root);
}

//	creates a parse handler for parsing a physical row trigger operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalRowTriggerParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerPhysicalRowTrigger(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a physical assert operator
CParseHandlerBase *
CParseHandlerFactory::CreatePhysicalAssertParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerAssert(mp, parse_handler_mgr, parse_handler_root);
}

// creates a trailing window frame edge parser
CParseHandlerBase *
CParseHandlerFactory::CreateFrameTrailingEdgeParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarWindowFrameEdge(mp, parse_handler_mgr, parse_handler_root, false /*fLeading*/);
}

// creates a leading window frame edge parser
CParseHandlerBase *
CParseHandlerFactory::CreateFrameLeadingEdgeParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarWindowFrameEdge(mp, parse_handler_mgr, parse_handler_root, true /*fLeading*/);
}

// creates a parse handler for parsing search strategy
CParseHandlerBase *
CParseHandlerFactory::CreateSearchStrategyParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSearchStrategy(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing search stage
CParseHandlerBase *
CParseHandlerFactory::CreateSearchStageParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerSearchStage(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing xform
CParseHandlerBase *
CParseHandlerFactory::CreateXformParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerXform(mp, parse_handler_mgr, parse_handler_root);
}

// creates cost params parse handler
CParseHandlerBase *
CParseHandlerFactory::CreateCostParamsParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCostParams(mp, parse_handler_mgr, parse_handler_root);
}

// creates cost param parse handler
CParseHandlerBase *
CParseHandlerFactory::CreateCostParamParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerCostParam(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for top level scalar expressions
CParseHandlerBase *
CParseHandlerFactory::CreateScExprParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarExpr(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific check constraint
CParseHandlerBase *
CParseHandlerFactory::CreateMDChkConstraintParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDGPDBCheckConstraint(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Values List operator
CParseHandlerBase *
CParseHandlerFactory::CreateScValuesListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerScalarValuesList(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing a Values Scan operator
CParseHandlerBase *
CParseHandlerFactory::CreateValuesScanParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerValuesScan(mp, parse_handler_mgr, parse_handler_root);
}

// creates a parse handler for parsing GPDB-specific array coerce cast metadata
CParseHandlerBase *
CParseHandlerFactory::CreateMDArrayCoerceCastParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_mgr,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerMDArrayCoerceCast(mp, parse_handler_mgr, parse_handler_root);
}

CParseHandlerBase *
CParseHandlerFactory::CreateNLJIndexParamListParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_manager,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerNLJIndexParamList(mp, parse_handler_manager, parse_handler_root);
}

CParseHandlerBase *
CParseHandlerFactory::CreateNLJIndexParamParseHandler
	(
	IMemoryPool *mp,
	CParseHandlerManager *parse_handler_manager,
	CParseHandlerBase *parse_handler_root
	)
{
	return GPOS_NEW(mp) CParseHandlerNLJIndexParam(mp, parse_handler_manager, parse_handler_root);
}
// EOF
